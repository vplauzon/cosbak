using System.IO;
using System.Threading.Tasks;
using Cosbak.Storage;

namespace Cosbak.Controllers.Backup
{
    internal class StoragePartitionController : IStoragePartitionController
    {
        private readonly string _partitionId;
        private readonly IStorageFacade _storage;
        private readonly ILogger _logger;
        private bool _created = false;
        private int _blockCount = 0;

        public StoragePartitionController(
            string partitionId,
            IStorageFacade storage,
            ILogger logger)
        {
            _partitionId = partitionId;
            _storage = storage;
            _logger = logger;
        }

        async Task IStoragePartitionController.WriteBatchAsync(Stream metaStream, Stream contentStream)
        {
            if (!_created)
            {
                await _storage.CreateAppendBlobAsync(_partitionId + ".meta");
                await _storage.CreateAppendBlobAsync(_partitionId + ".content");
                _created = true;
            }

            await _storage.AppendBlobAsync(_partitionId + ".meta", metaStream);
            await _storage.AppendBlobAsync(_partitionId + ".content", contentStream);
            ++_blockCount;
        }
    }
}