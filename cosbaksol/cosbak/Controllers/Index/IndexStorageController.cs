using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cosbak.Storage;

namespace Cosbak.Controllers.Index
{
    public class IndexStorageController : IIndexStorageController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly ILogger _logger;
        private readonly string _cosmosAccountName;
        private readonly CollectionFilter _collectionFilter;

        public IndexStorageController(
            IStorageFacade storageFacade,
            ILogger logger,
            string cosmosAccountName,
            IEnumerable<string> filters)
        {
            _storageFacade = storageFacade ?? throw new ArgumentNullException(nameof(storageFacade));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _cosmosAccountName = cosmosAccountName;
            _collectionFilter = new CollectionFilter(filters);
        }

        async Task<IImmutableList<IIndexCollectionBackupController>> IIndexStorageController.GetCollectionsAsync()
        {
            var accounts = _storageFacade.ChangeFolder(Constants.ACCOUNTS_FOLDER);
            var masterPaths = await accounts.ListBlobsAsync(path => path.EndsWith(Constants.BACKUP_MASTER));
            var controllers = from i in from path in masterPaths
                                        let parts = path.Split('/')
                                        where parts.Length == 5
                                        select new { Path = path, Parts = parts }
                              let account = i.Parts[0]
                              let db = i.Parts[1]
                              let collection = i.Parts[2]
                              where account == _cosmosAccountName
                              && _collectionFilter.IsIncluded(db, collection)
                              let controllerFacade = accounts
                              .ChangeFolder(account)
                              .ChangeFolder(db)
                              .ChangeFolder(collection)
                              .ChangeFolder(Constants.BACKUP_FOLDER)
                              select new IndexCollectionBackupController(
                                  account,
                                  db,
                                  collection,
                                  controllerFacade);

            return controllers.Cast<IIndexCollectionBackupController>().ToImmutableArray();
        }
    }
}