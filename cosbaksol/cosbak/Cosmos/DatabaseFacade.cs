using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    internal class DatabaseFacade : IDatabaseFacade
    {
        private readonly Database _database;
        private readonly CosmosAccountFacade _parent;
        private readonly ILogger _logger;

        public DatabaseFacade(
            Database database,
            CosmosAccountFacade parent,
            ILogger logger)
        {
            _database = database;
            _parent = parent;
            _logger = logger.AddContext("database", database.Id);
        }

        ICosmosAccountFacade IDatabaseFacade.Parent => _parent;

        string IDatabaseFacade.DatabaseName => _database.Id;

        async Task<IEnumerable<ICollectionFacade>> IDatabaseFacade.GetCollectionsAsync()
        {
            var iterator = _database.GetContainerQueryIterator<dynamic>(
                "SELECT c.id, c.partitionKey.paths[0] as partitionKey FROM c");
            var collections = await QueryHelper.GetAllResultsAsync(iterator);
            var collectionFacades = from coll in collections.Content
                                    select new CollectionFacade(
                                        _database.GetContainer((string)coll.id),
                                        (string)coll.partitionKey,
                                        this,
                                        _logger);

            return collectionFacades.ToArray<ICollectionFacade>();
        }
    }
}