using System.Collections.Generic;
using Microsoft.Azure.Documents.Client;

namespace Cosbak.Cosmos
{
    internal class DatabaseGateway : IDatabaseGateway
    {
        private DocumentClient _client;
        private CosmosDbGateway cosmosDbGateway;
        private readonly string _name;
        private string[] v;

        public DatabaseGateway(DocumentClient client, CosmosDbGateway cosmosDbGateway, string name, string[] v)
        {
            _client = client;
            this.cosmosDbGateway = cosmosDbGateway;
            this._name = name;
            this.v = v;
        }

        string IDatabaseGateway.Name => _name;

        IEnumerable<ICollectionGateway> IDatabaseGateway.GetCollectionsAsync()
        {
            throw new System.NotImplementedException();
        }
    }
}