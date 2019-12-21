using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.LogBackup
{
    internal class LogCollectionBackupController
    {
        private const int MAX_BATCH_SIZE = 2;
        private const int MAX_BLOCK_COUNT = 1000;
        private const long MAX_DOCUMENT_LOG_SIZE = 1024 * 1024 * 1024;
        private static readonly TimeSpan MAX_CHECKPOINT_AGE = TimeSpan.FromDays(1);

        private readonly LogFile _logFile;
        private readonly BackupPlan _plan;
        private readonly ILogger _logger;

        public LogCollectionBackupController(
            ICollectionFacade collectionFacade,
            IStorageFacade storageFacade,
            BackupPlan plan,
            ILogger logger)
        {
            Collection = collectionFacade;
            _logFile = new LogFile(
                storageFacade,
                collectionFacade.Parent.Parent.AccountName,
                collectionFacade.Parent.DatabaseName,
                collectionFacade.CollectionName,
                logger);
            _plan = plan;
            _logger = logger;
        }

        public ICollectionFacade Collection { get; }

        public async Task InitializeAsync()
        {
            await _logFile.InitializeAsync();
        }

        public async Task<LogBatchResult> LogBatchAsync()
        {
            var previousTimeStamp = _logFile.LastTimeStamp;
            var timeWindow = await Collection.SizeTimeWindowAsync(
                previousTimeStamp,
                MAX_BATCH_SIZE);

            _logger
                .AddContext("CurrentTimeStamp", timeWindow.currentTimeStamp)
                .AddContext("DocumentCount", timeWindow.count)
                .AddContext("MaxTimeStamp", timeWindow.maxTimeStamp)
                .WriteEvent("LogBatchTimeWindow");
            if (timeWindow.count != 0)
            {
                _logger.WriteEvent("LogDocumentBatch-Start");
                await LogDocumentBatchAsync(previousTimeStamp, timeWindow.maxTimeStamp);
                _logger.WriteEvent("LogDocumentBatch-End");
            }
            else
            {
                _logger.Display("No document to backup");
                _logger.WriteEvent("No-document-to-backup");
            }
            var hasCaughtUp = timeWindow.currentTimeStamp == timeWindow.maxTimeStamp;

            if (hasCaughtUp
                && IsCheckPointTime(_logFile.LastCheckpointTimeStamp, timeWindow.currentTimeStamp))
            {
                await LogCheckPointAsync(timeWindow.currentTimeStamp);
            }
            await _logFile.PersistAsync();

            return new LogBatchResult(
                hasCaughtUp,
                _logFile.TotalDocumentBlockCount > MAX_BLOCK_COUNT
                || _logFile.TotalDocumentSize > MAX_DOCUMENT_LOG_SIZE,
                _logFile.TotalCheckPointBlockCount > MAX_BLOCK_COUNT
                || _logFile.TotalCheckPointSize > MAX_DOCUMENT_LOG_SIZE);
        }

        private async Task LogCheckPointAsync(long currentTimeStamp)
        {
            _logger.Display("Preparing Checkpoint...");
            _logger.WriteEvent("Checkpoint-Start");

            var idsBlockNames = _plan.Included.ExplicitDelete
                ? await WriteIteratorToBlocksAsync(Collection.GetAllIds(), "LogAllIds")
                : null;
            var sprocsBlockNames = _plan.Included.Sprocs
                ? await WriteIteratorToBlocksAsync(Collection.GetAllStoredProcedures(), "LogAllSprocs")
                : null;
            var functionsBlockNames = _plan.Included.Functions
                ? await WriteIteratorToBlocksAsync(Collection.GetAllFunctions(), "LogAllFunctions")
                : null;
            var triggersBlockNames = _plan.Included.Triggers
                ? await WriteIteratorToBlocksAsync(Collection.GetAllTriggers(), "LogAllTriggers")
                : null;

            _logFile.CreateCheckpoint(
                currentTimeStamp,
                idsBlockNames,
                sprocsBlockNames,
                functionsBlockNames,
                triggersBlockNames);
            _logger.WriteEvent("Checkpoint-End");
        }

        private bool IsCheckPointTime(long previousTimeStamp, long currentTimeStamp)
        {
            var delta = TimeSpan.FromSeconds(currentTimeStamp - previousTimeStamp);

            return delta >= _plan.Rpo;
        }

        private async Task LogDocumentBatchAsync(long previousLastUpdateTime, long maxTimeStamp)
        {
            var iterator = Collection.GetTimeWindowDocuments(
                previousLastUpdateTime,
                maxTimeStamp);
            var blocks = await WriteIteratorToBlocksAsync(iterator, "LogDocumentBatch");

            _logFile.AddDocumentBatch(maxTimeStamp, blocks);
        }

        private async Task<IImmutableList<Block>> WriteIteratorToBlocksAsync(
            StreamIterator iterator,
            string eventName)
        {
            var buffer = new byte[Constants.MAX_LOG_BLOCK_SIZE];
            var blocks = ImmutableList<Block>.Empty;
            double ru = 0;
            var resultCount = 0;
            var blockCount = 0;
            int index = 0;

            while (iterator.HasMoreResults)
            {
                var result = await iterator.ReadNextAsync();

                if (result.Stream.Length > buffer.Length)
                {
                    throw new NotSupportedException(
                        $"Query return bigger than buffer:  {result.Stream.Length}");
                }
                if (result.Stream.Length > buffer.Length - index)
                {
                    var block = await _logFile.WriteBlockAsync(buffer, index);

                    blocks = blocks.Add(block);
                    ++blockCount;
                    index = 0;
                }

                var memory = new Memory<byte>(buffer, index, (int)result.Stream.Length);

                await result.Stream.ReadAsync(memory);
                index += memory.Length;
                ++resultCount;
                ru += result.RequestCharge;
            }
            if (index > 0)
            {
                var block = await _logFile.WriteBlockAsync(buffer, index);

                blocks = blocks.Add(block);
                ++blockCount;
            }
            _logger
                .AddContext("ru", ru)
                .AddContext("blockCount", blockCount)
                .WriteEvent(eventName);

            return blocks;
        }

        public async Task DisposeAsync()
        {
            await _logFile.DisposeAsync();
        }
    }
}