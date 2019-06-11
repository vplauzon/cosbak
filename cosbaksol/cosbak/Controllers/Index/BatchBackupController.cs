using Cosbak.Storage;

namespace Cosbak.Controllers.Index
{
    public class BatchBackupController: IBatchBackupController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly long _timeStamp;

        public BatchBackupController(IStorageFacade storageFacade, long timeStamp)
        {
            _storageFacade = storageFacade;
            _timeStamp = timeStamp;
        }
    }
}