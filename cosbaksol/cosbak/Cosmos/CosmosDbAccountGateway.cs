using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Cosbak.Cosmos
{
    internal class CosmosDbAccountGateway : ICosmosDbAccountGateway
    {
        private readonly string _accountName;
        private readonly DocumentClient _client;
        private readonly IImmutableDictionary<string, string[]> _filters;

        public CosmosDbAccountGateway(string accountName, string key, string[] filters)
        {
            _accountName = accountName;
            _client = new DocumentClient(
                new Uri($"https://{_accountName}.documents.azure.com:443/"),
                key);
            _filters = ParseFilters(filters);
        }

        string ICosmosDbAccountGateway.AccountName => _accountName;

        async Task<IEnumerable<IDatabaseGateway>> ICosmosDbAccountGateway.GetDatabasesAsync()
        {
            var query = _client.CreateDatabaseQuery();
            var dbs = await QueryHelper.GetAllResultsAsync(query.AsDocumentQuery());

            if (_filters.Any())
            {
                var filteredDbs = from db in dbs
                                  where _filters.ContainsKey(db.Id)
                                  select new DatabaseGateway(_client, db.Id, this, _filters[db.Id]);
                var gateways = filteredDbs.ToArray<IDatabaseGateway>();

                if (gateways.Length != _filters.Count)
                {
                    var set = ImmutableSortedSet.Create(gateways.Select(g => g.DatabaseName).ToArray());
                    var notFound = from f in _filters
                                   where !set.Contains(f.Key)
                                   select f.Key;

                    throw new CosbakException($"Database '{notFound.First()}' not found in account '{_accountName}'");
                }

                return gateways;
            }
            else
            {
                var gateways = dbs
                    .Select(db => new DatabaseGateway(_client, db.Id, this, new string[0]))
                    .ToArray<IDatabaseGateway>();

                return gateways;
            }
        }

        private IImmutableDictionary<string, string[]> ParseFilters(string[] filters)
        {
            if (filters == null || filters.Length == 0)
            {
                return ImmutableDictionary<string, string[]>.Empty;
            }
            else
            {
                var pairs = from f in filters
                            let array = f.Split('.')
                            let pair = new { Db = array[0], Collection = array.Length > 1 ? array[1] : null }
                            select pair;
                var grouping = from p in pairs
                               group p by p.Db into g
                               select g;
                var dictionary = grouping.ToImmutableSortedDictionary(
                    g => g.Key,
                    g => AccountForNoCollections(g.Select(i => i.Collection).ToArray()));

                return dictionary;
            }
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