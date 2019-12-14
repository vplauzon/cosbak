using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Cosbak.Cosmos
{
    internal class DatabaseAccountFacade : IDatabaseAccountFacade
    {
        private readonly string _accountName;
        private readonly DocumentClient _client;

        public DatabaseAccountFacade(string accountName, string key)
        {
            _accountName = accountName;
            _client = new DocumentClient(
                new Uri($"https://{_accountName}.documents.azure.com:443/"),
                key);
        }

        string IDatabaseAccountFacade.AccountName => _accountName;

        async Task<IEnumerable<IDatabaseFacade>> IDatabaseAccountFacade.GetDatabasesAsync()
        {
            var query = _client.CreateDatabaseQuery();
            var dbs = await QueryHelper.GetAllResultsAsync(query.AsDocumentQuery());

                var gateways = dbs
                    .Select(db => new DatabaseFacade(_client, db.Id, this, new string[0]))
                    .ToArray<IDatabaseFacade>();

                return gateways;
        }

        private string[] AccountForNoCollections(string[] collections)
        {
            if (collections.Any(c => c == null))
            {
                return new string[0];
            }
            else
            {
                return collections;
            }
        }
    }
}