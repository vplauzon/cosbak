using System.Collections.Generic;
using System.Collections.Immutable;
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

        async Task<IImmutableList<IBatchBackupController>>
            ICollectionBackupController.GetUnprocessedBatchesAsync()
        {
            var masterContent = await _storageFacade.GetContentAsync(Constants.BACKUP_MASTER);
            var serializer = new JsonSerializer();

            using (var stringReader = new StringReader(masterContent))
            using (var reader = new JsonTextReader(stringReader))
            {
                var master = serializer.Deserialize<MasterBackupData>(reader);

                if (master == null)
                {
                    return ImmutableArray<IBatchBackupController>.Empty;
                }
                else
                {
                    var controllers =
                        from batch in master.Batches
                        let folderId = batch.FolderId.ToString()
                        let folderFacade = _storageFacade.ChangeFolder(folderId)
                        select new BatchBackupController(
                            folderFacade,
                            batch.FolderId,
                            batch.TimeStamp);

                    return controllers.Cast<IBatchBackupController>().ToImmutableArray();
                }
            }
        }

        async Task<IBlobIndexController>
            ICollectionBackupController.GetCurrentBlobIndexControllerAsync(long firstTimeStamp)
        {
            return await BlobIndexController.GetCurrentOrNewAsync(
                _storageFacade.ChangeFolder(Constants.INDEX_FOLDER),
                firstTimeStamp);
        }
    }
}