using Cosbak.Config;
using Cosbak.Storage;

namespace Cosbak.Storage
{
    internal class StorageGateway : IStorageGateway
    {
        private StorageDescription storage;

        public StorageGateway(StorageDescription storage)
        {
            this.storage = storage;
        }
    }
}