using Cosbak.Storage;
using System;
using System.Collections.Generic;

namespace Cosbak.Controllers.Backup
{
    public class BackupStorageController : IBackupStorageController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly ILogger _logger;

        public BackupStorageController(IStorageFacade storageFacade, ILogger logger)
        {
            _storageFacade = storageFacade;
            _logger = logger;
        }
    }
}