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

        public DatabaseFacade(
            Database database,
            CosmosAccountFacade parent)
        {
            _database = database;
            _parent = parent;
        }

        ICosmosAccountFacade IDatabaseFacade.Parent => _parent;

        string IDatabaseFacade.DatabaseName => _database.Id;

        async Task<IEnumerable<ICollectionFacade>> IDatabaseFacade.GetCollectionsAsync()
        {
            var iterator = _database.GetContainerQueryIterator<dynamic>(
                "SELECT c.id, c.partitionKey.paths[0] as partitionKey FROM c");
            var collections = await QueryHelper.GetAllResultsAsync(iterator);
            var collectionFacades = from coll in collections
                                    select new CollectionFacade(
                                        _database.GetContainer((string)coll.id),
                                        (string)coll.partitionKey,
                                        this);

            return collectionFacades.ToArray<ICollectionFacade>();
        }
    }
}