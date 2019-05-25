using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Storage
{
    public class StorageBackupGateway : IStorageBackupGateway
    {
        private readonly IStorageFacade _storageFacade;

        public StorageBackupGateway(IStorageFacade storageFacade)
        {
            _storageFacade = storageFacade;
        }
    }
}