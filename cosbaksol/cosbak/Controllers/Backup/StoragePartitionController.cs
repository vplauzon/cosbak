using Cosbak.Storage;

namespace Cosbak.Controllers.Backup
{
    internal class StoragePartitionController : IStoragePartitionController
    {
        private readonly string _partitionId;
        private readonly IStorageFacade _contentStorage;
        private readonly ILogger _logger;

        public StoragePartitionController(
            string partitionId,
            IStorageFacade contentStorage,
            ILogger logger)
        {
            _partitionId = partitionId;
            _contentStorage = contentStorage;
            _logger = logger;
        }
    }
}