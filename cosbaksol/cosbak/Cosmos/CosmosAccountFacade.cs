using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Cosbak.Cosmos
{
    internal class CosmosAccountFacade : ICosmosAccountFacade
    {
        private readonly string _accountName;
        private readonly DocumentClient _client;

        public CosmosAccountFacade(string accountName, string key)
        {
            _accountName = accountName;
            _client = new DocumentClient(
                new Uri($"https://{_accountName}.documents.azure.com:443/"),
                key);
        }

        string ICosmosAccountFacade.AccountName => _accountName;

        async Task<IEnumerable<IDatabaseFacade>> ICosmosAccountFacade.GetDatabasesAsync()
        {
            var query = _client.CreateDatabaseQuery();
            var dbs = await QueryHelper.GetAllResultsAsync(query.AsDocumentQuery());

            var gateways = dbs
                .Select(db => new DatabaseFacade(_client, db.Id, this))
                .ToArray<IDatabaseFacade>();

            return gateways;
        }
    }
}