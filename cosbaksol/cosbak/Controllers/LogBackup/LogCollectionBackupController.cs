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
        private const int MAX_BATCH_SIZE = 10;

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

        public async Task BackupBatchAsync()
        {
            var previousLastUpdateTime = _logFile.LastUpdateTime;
            var lastUpdateTime = await _collectionFacade.GetLastUpdateTimeAsync(
                previousLastUpdateTime,
                MAX_BATCH_SIZE);

            if (lastUpdateTime == null)
            {
            }
            else
            {
                var buffer = new byte[Constants.MAX_LOG_BLOCK_SIZE];
                var blockNames = ImmutableList<string>.Empty;
                var iterator = _collectionFacade.GetTimeWindowDocuments(
                    previousLastUpdateTime,
                    lastUpdateTime.Value);
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
                _logFile.AddDocumentBatch(lastUpdateTime.Value, blockNames);
                await _logFile.PersistAsync();
            }
        }

        public async Task DisposeAsync()
        {
            await _logFile.DisposeAsync();
        }
    }
}