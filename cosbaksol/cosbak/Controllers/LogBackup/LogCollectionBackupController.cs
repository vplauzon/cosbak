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

        private readonly ICollectionFacade _collectionFacade;
        private readonly LogFile _logFile;
        private readonly ILogger _logger;

        public LogCollectionBackupController(
            ICollectionFacade collectionFacade,
            IStorageFacade storageFacade,
            ILogger logger)
        {
            _collectionFacade = collectionFacade;
            _logFile = new LogFile(
                storageFacade,
                collectionFacade.Parent.Parent.AccountName,
                collectionFacade.Parent.DatabaseName,
                collectionFacade.CollectionName,
                logger);
            _logger = logger;
        }

        public async Task InitializeAsync()
        {
            await _logFile.InitializeAsync();
        }

        public async Task LogBatchAsync()
        {
            var previousLastUpdateTime = _logFile.LastUpdateTime;
            var timeWindow = await _collectionFacade.SizeTimeWindowAsync(
                previousLastUpdateTime,
                MAX_BATCH_SIZE);

            _logger
                .AddContext("CurrentTimeStamp", timeWindow.currentTimeStamp)
                .AddContext("DocumentCount", timeWindow.count)
                .AddContext("MaxTimeStamp", timeWindow.maxTimeStamp)
                .WriteEvent("LogBatchTimeWindow");
            if (timeWindow.count != 0)
            {
                await LogDocumentBatchAsync(previousLastUpdateTime, timeWindow.maxTimeStamp);
            }
            //await _logFile.PersistAsync();
        }

        private async Task LogDocumentBatchAsync(long previousLastUpdateTime, long maxTimeStamp)
        {
            var buffer = new byte[Constants.MAX_LOG_BLOCK_SIZE];
            var blockNames = ImmutableList<string>.Empty;
            var iterator = _collectionFacade.GetTimeWindowDocuments(
                previousLastUpdateTime,
                maxTimeStamp);
            int index = 0;

            while (iterator.HasMoreResults)
            {
                var stream = await iterator.ReadNextAsync();

                if (stream.Length > buffer.Length)
                {
                    throw new NotSupportedException(
                        $"Query return bigger than buffer:  {stream.Length}");
                }
                if (stream.Length > buffer.Length - index)
                {
                    var blockName = await _logFile.WriteBlockAsync(buffer, index);

                    blockNames = blockNames.Add(blockName);
                    index = 0;
                }

                var memory = new Memory<byte>(buffer, index, (int)stream.Length);

                await stream.ReadAsync(memory);
                index += memory.Length;
            }
            if (index > 0)
            {
                var blockName = await _logFile.WriteBlockAsync(buffer, index);

                blockNames = blockNames.Add(blockName);
            }
            _logFile.AddDocumentBatch(maxTimeStamp, blockNames);
        }

        public async Task DisposeAsync()
        {
            await _logFile.DisposeAsync();
        }
    }
}