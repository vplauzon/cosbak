using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cosbak.Config;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Cosbak.Cosmos
{
    internal class CosmosDbGateway : ICosmosDbGateway
    {
        private readonly string _accountName;
        private readonly DocumentClient _client;
        private readonly string[] _filters;

        public CosmosDbGateway(AccountDescription description)
        {
            _accountName = description.Name;
            _client = new DocumentClient(
                new Uri($"https://{_accountName}.documents.azure.com:443/"),
                description.Key);
            _filters = description.Filters;
        }

        async Task<IEnumerable<IDatabase>> ICosmosDbGateway.GetDatabasesAsync()
        {
            var query = _client.CreateDatabaseQuery();
            var dbs = await QueryHelper.GetAllResultsAsync(query.AsDocumentQuery());
            
            throw new NotImplementedException();
        }
    }
}