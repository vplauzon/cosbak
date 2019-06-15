using System.Threading.Tasks;
using Cosbak.Storage;

namespace Cosbak.Controllers.Index
{
    internal class PartitionBackupController : IPartitionBackupController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly string _partitionId;

        public PartitionBackupController(IStorageFacade storageFacade, string partitionId)
        {
            _storageFacade = storageFacade;
            _partitionId = partitionId;
        }

        string IPartitionBackupController.PartitionId => _partitionId;

        public async Task<int> LoadIndexAsync(byte[] indexBuffer)
        {
            var path = _partitionId + ".meta";

            return await _storageFacade.DownloadRangeAsync(path, indexBuffer);
        }
    }
}