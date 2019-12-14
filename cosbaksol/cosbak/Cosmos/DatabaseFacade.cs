using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Cosbak.Cosmos
{
    internal class DatabaseFacade : IDatabaseFacade
    {
        private readonly DocumentClient _client;
        private readonly string _databaseName;
        private readonly CosmosAccountFacade _parent;

        public DatabaseFacade(
            DocumentClient client,
            string databaseName,
            CosmosAccountFacade parent)
        {
            _databaseName = databaseName;
            _client = client;
            _parent = parent;
        }

        ICosmosAccountFacade IDatabaseFacade.Parent => _parent;

        string IDatabaseFacade.DatabaseName => _databaseName;

        async Task<IEnumerable<ICollectionFacade>> IDatabaseFacade.GetCollectionsAsync()
        {
            var databaseUri = UriFactory.CreateDatabaseUri(_databaseName);
            var query = _client.CreateDocumentCollectionQuery(databaseUri);
            var collections = await QueryHelper.GetAllResultsAsync(query.AsDocumentQuery());
            var collectionFacades = from coll in collections
                                    select new CollectionFacade(
                                        _client,
                                        coll.Id,
                                        coll.PartitionKey.Paths.First(),
                                        this);

            return collectionFacades.ToArray<ICollectionFacade>();
        }
    }
}