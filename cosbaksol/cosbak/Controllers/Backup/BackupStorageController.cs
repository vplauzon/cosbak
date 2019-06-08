using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public class BackupStorageController : IBackupStorageController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly ILogger _logger;

        public BackupStorageController(IStorageFacade storageFacade, ILogger logger)
        {
            _storageFacade = storageFacade ?? throw new ArgumentNullException(nameof(storageFacade));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        async Task<IStorageCollectionController> IBackupStorageController.LockMasterAsync(
            string account,
            string database,
            string collection)
        {
            var backupFolder = _storageFacade.ChangeFolder(
                $"{Constants.ACCOUNTS_FOLDER}/{account}/{database}/{collection}/{Constants.BACKUP_FOLDER}");
            var collectionController = new StorageCollectionController(backupFolder, _logger);

            await collectionController.InitializeAsync();

            return collectionController;
        }
    }
}