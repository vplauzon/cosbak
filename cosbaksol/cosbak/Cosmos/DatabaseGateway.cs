using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.Azure.Documents.Client;

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

        IEnumerable<ICollectionGateway> IDatabaseGateway.GetCollectionsAsync()
        {
            throw new System.NotImplementedException();
        }
    }
}