using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
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
            var lastUpdateTime = await _collectionFacade.GetLastUpdateTimeAsync(0, MAX_BATCH_SIZE);

            throw new NotImplementedException();
        }

        public async Task DisposeAsync()
        {
            await _logFile.DisposeAsync();
        }
    }
}