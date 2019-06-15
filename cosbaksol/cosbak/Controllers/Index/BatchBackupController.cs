using Cosbak.Storage;

namespace Cosbak.Controllers.Index
{
    public class BatchBackupController : IBatchBackupController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly int _folderId;
        private readonly long _timeStamp;

        public BatchBackupController(
            IStorageFacade storageFacade,
            int folderId,
            long timeStamp)
        {
            _storageFacade = storageFacade;
            _folderId = folderId;
            _timeStamp = timeStamp;
        }

        long IBatchBackupController.TimeStamp => _timeStamp;

        int IBatchBackupController.FolderId => _folderId;
    }
}