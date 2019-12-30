using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    internal class CosmosAccountFacade : ICosmosAccountFacade
    {
        private readonly string _accountName;
        private readonly CosmosClient _client;
        private readonly ILogger _logger;

        public CosmosAccountFacade(string accountName, string key, ILogger logger)
        {
            var options = new CosmosClientOptions
            {
                AllowBulkExecution = true
            };

            _accountName = accountName;
            _client = new CosmosClient(
                $"https://{_accountName}.documents.azure.com:443/",
                key,
                options);
            _logger = logger.AddContext("account", accountName);
        }

        string ICosmosAccountFacade.AccountName => _accountName;

        async Task<IEnumerable<IDatabaseFacade>> ICosmosAccountFacade.GetDatabasesAsync()
        {
            var iterator = _client.GetDatabaseQueryIterator<string>("SELECT VALUE d.id FROM d");
            var ids = await QueryHelper.GetAllResultsAsync(iterator);

            var gateways = ids
                .Content
                .Select(id => new DatabaseFacade(_client.GetDatabase(id), this, _logger))
                .ToArray<IDatabaseFacade>();

            return gateways;
        }
    }
}