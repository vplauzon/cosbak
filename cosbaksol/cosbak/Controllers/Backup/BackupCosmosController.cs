using Cosbak.Cosmos;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public class BackupCosmosController : IBackupCosmosController
    {
        private readonly IDatabaseAccountFacade _accountFacade;
        private readonly ILogger _logger;
        private readonly CollectionFilter _collectionFilter;

        #region Constructors
        public BackupCosmosController(
            IDatabaseAccountFacade accountFacade,
            ILogger logger,
            IEnumerable<string> filters)
        {
            _accountFacade = accountFacade ?? throw new ArgumentNullException(nameof(accountFacade));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _collectionFilter = new CollectionFilter(filters);
        }
        #endregion

        async Task<IImmutableList<ICosmosCollectionController>> IBackupCosmosController.GetCollectionsAsync()
        {
            var builder = ImmutableList<ICosmosCollectionController>.Empty.ToBuilder();

            foreach (var db in await _accountFacade.GetDatabasesAsync())
            {
                foreach (var collection in await db.GetCollectionsAsync())
                {
                    if (_collectionFilter.IsIncluded(db.DatabaseName, collection.CollectionName))
                    {
                        var controller = new CosmosCollectionController(collection, _logger);

                        builder.Add(controller);
                    }
                }
            }

            return builder.ToImmutableArray();
        }
    }
}