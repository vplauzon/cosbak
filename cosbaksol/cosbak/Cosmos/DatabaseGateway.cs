using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Cosbak.Cosmos
{
    internal class DatabaseGateway : IDatabaseGateway
    {
        private readonly DocumentClient _client;
        private readonly string _databaseName;
        private readonly CosmosDbAccountGateway _parent;
        private readonly IImmutableList<string> _filters;

        public DatabaseGateway(DocumentClient client, string databaseName, CosmosDbAccountGateway parent, string[] filters)
        {
            _databaseName = databaseName;
            _client = client;
            _parent = parent;
            _filters = ImmutableArray<string>.Empty.AddRange(filters);
        }

        ICosmosDbAccountGateway IDatabaseGateway.Parent => _parent;

        string IDatabaseGateway.DatabaseName => _databaseName;

        async Task<IEnumerable<ICollectionGateway>> IDatabaseGateway.GetCollectionsAsync()
        {
            var databaseUri = UriFactory.CreateDatabaseUri(_databaseName);
            var query = _client.CreateDocumentCollectionQuery(databaseUri);
            var collections = await QueryHelper.GetAllResultsAsync(query.AsDocumentQuery());

            if (_filters.Any())
            {
                var filteredCollections = from coll in collections
                                          where _filters.Contains(coll.Id)
                                          select new CollectionGateway(_client, coll.Id, coll.PartitionKey.Paths.First(), this);
                var gateways = filteredCollections.ToArray<ICollectionGateway>();

                if (gateways.Length != _filters.Count)
                {
                    var set = ImmutableSortedSet.Create(gateways.Select(g => g.CollectionName).ToArray());
                    var notFound = from f in _filters
                                   where !set.Contains(f)
                                   select f;

                    throw new BackupException($"Collection '{notFound.First()}' not found in database '{_databaseName}'");
                }

                return gateways;
            }
            else
            {
                var gateways = collections
                    .Select(coll => new CollectionGateway(_client, coll.Id, coll.PartitionKey.Paths.First(), this))
                    .ToArray<ICollectionGateway>();

                return gateways;
            }
        }
    }
}