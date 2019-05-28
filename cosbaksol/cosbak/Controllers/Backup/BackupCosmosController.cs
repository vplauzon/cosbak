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
        private readonly IImmutableDictionary<string, IImmutableSet<string>> _filterMap;

        #region Constructors
        public BackupCosmosController(
            IDatabaseAccountFacade accountFacade,
            ILogger logger,
            IEnumerable<string> filters)
        {
            _accountFacade = accountFacade ?? throw new ArgumentNullException(nameof(accountFacade));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _filterMap = CreateFilterMap(filters);
        }

        private static IImmutableDictionary<string, IImmutableSet<string>> CreateFilterMap(
            IEnumerable<string> filters)
        {
            if (filters == null)
            {
                return ImmutableDictionary<string, IImmutableSet<string>>.Empty;
            }
            else
            {
                var list = from f in filters
                           let parts = f.Split(".")
                           let db = parts[0]
                           let collection = parts[1].Trim()
                           group collection by db;
                var dbMap = list.ToImmutableDictionary(
                    g => g.Key,
                    g => g.ToImmutableHashSet() as IImmutableSet<string>);

                return dbMap;
            }
        }
        #endregion

        async Task<IImmutableList<ICosmosCollectionController>> IBackupCosmosController.GetCollectionsAsync()
        {
            var builder = ImmutableList<ICosmosCollectionController>.Empty.ToBuilder();

            foreach (var db in await _accountFacade.GetDatabasesAsync())
            {
                foreach (var collection in await db.GetCollectionsAsync())
                {
                    if (IsIncluded(collection))
                    {
                        var controller = new CosmosCollectionController(collection, _logger);

                        builder.Add(controller);
                    }
                }
            }

            return builder.ToImmutableArray();
        }

        private bool IsIncluded(ICollectionFacade collection)
        {
            var db = collection.Parent.DatabaseName;

            if (!_filterMap.Any())
            {
                return true;
            }
            else if (!_filterMap.ContainsKey(db))
            {
                return false;
            }
            else
            {
                var collectionFilter = _filterMap[db];

                return collectionFilter.Contains("*")
                    || collectionFilter.Contains(collection.CollectionName);
            }
        }
    }
}