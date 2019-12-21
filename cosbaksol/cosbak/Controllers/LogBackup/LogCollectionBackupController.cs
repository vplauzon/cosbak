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
            var hasLoggedUntilNow = timeWindow.currentTimeStamp == timeWindow.maxTimeStamp;

            if (hasLoggedUntilNow
                && IsCheckPointTime(_logFile.LastCheckpointTimeStamp, timeWindow.currentTimeStamp))
            {
                _logger.Display("Preparing Checkpoint...");
                _logger.WriteEvent("Checkpoint-Start");
                await LogCheckPointAsync(timeWindow.currentTimeStamp);
                _logger.WriteEvent("Checkpoint-End");
            }
            await _logFile.PersistAsync();

            return new LogBatchResult(hasLoggedUntilNow);
        }

        private async Task LogCheckPointAsync(long currentTimeStamp)
        {
            var idsBlockNames = _plan.Included.ExplicitDelete
                ? await WriteIteratorToBlocksAsync(Collection.GetAllIds(), "LogAllIds")
                : null;
            var sprocsBlockNames = _plan.Included.Sprocs
                ? await WriteIteratorToBlocksAsync(Collection.GetAllStoredProcedures(), "LogAllSprocs")
                : null;

            _logFile.CreateCheckpoint(currentTimeStamp, idsBlockNames, sprocsBlockNames);
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
            var blockNames = await WriteIteratorToBlocksAsync(iterator, "LogDocumentBatch");

            _logFile.AddDocumentBatch(maxTimeStamp, blockNames);
        }

        private async Task<ImmutableList<string>> WriteIteratorToBlocksAsync(
            StreamIterator iterator,
            string eventName)
        {
            var buffer = new byte[Constants.MAX_LOG_BLOCK_SIZE];
            var blockNames = ImmutableList<string>.Empty;
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
                    var blockName = await _logFile.WriteBlockAsync(buffer, index);

                    blockNames = blockNames.Add(blockName);
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
                var blockName = await _logFile.WriteBlockAsync(buffer, index);

                blockNames = blockNames.Add(blockName);
                ++blockCount;
            }
            _logger
                .AddContext("ru", ru)
                .AddContext("blockCount", blockCount)
                .WriteEvent(eventName);

            return blockNames;
        }

        public async Task DisposeAsync()
        {
            await _logFile.DisposeAsync();
        }
    }
}