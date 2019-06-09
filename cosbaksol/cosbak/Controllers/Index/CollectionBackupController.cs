using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cosbak.Storage;
using Newtonsoft.Json;

namespace Cosbak.Controllers.Index
{
    public class CollectionBackupController : ICollectionBackupController
    {
        private readonly string _account;
        private readonly string _db;
        private readonly string _collection;
        private readonly IStorageFacade _storageFacade;

        public CollectionBackupController(
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

        string ICollectionBackupController.Account => _account;

        string ICollectionBackupController.Database => _db;

        string ICollectionBackupController.Collection => _collection;

        async Task<IEnumerable<IBatchBackupController>> ICollectionBackupController.GetBatchesAsync()
        {
            var masterContent = await _storageFacade.GetContentAsync(Constants.BACKUP_MASTER);
            var serializer = new JsonSerializer();

            using (var stringReader = new StringReader(masterContent))
            using (var reader = new JsonTextReader(stringReader))
            {
                var master = serializer.Deserialize<MasterBackupData>(reader);

                if (master == null)
                {
                    return new IBatchBackupController[0];
                }
                else
                {
                    var q = from folder in master.ContentFolders
                            select folder;

                    throw new System.NotImplementedException();
                }
            }
        }
    }
}