using Cosbak.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Storage
{
    public class StorageBackupGateway : IStorageBackupGateway
    {
        private readonly IStorageFacade _storageFacade;
        private readonly ILogger _logger;

        public StorageBackupGateway(IStorageFacade storageFacade, ILogger logger)
        {
            _storageFacade = storageFacade;
            _logger = logger;
        }
    }
}