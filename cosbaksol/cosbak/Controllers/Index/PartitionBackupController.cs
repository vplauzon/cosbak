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
    }
}