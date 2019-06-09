using Cosbak.Storage;

namespace Cosbak.Controllers.Index
{
    public class IndexCollectionBackupController : IIndexCollectionBackupController
    {
        private readonly string _account;
        private readonly string _db;
        private readonly string _collection;
        private readonly IStorageFacade _storageFacade;

        public IndexCollectionBackupController(
            string account,
            string db,
            string collection,
            IStorageFacade storageFacade)
        {
            _account = account;
            _db = db;
            _collection = collection;
            _storageFacade = storageFacade;
        }

        string IIndexCollectionBackupController.Account => _account;

        string IIndexCollectionBackupController.Database => _db;

        string IIndexCollectionBackupController.Collection => _collection;
    }
}