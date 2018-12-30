using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
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
        private readonly IImmutableDictionary<string, string[]> _filters;

        public CosmosDbGateway(AccountDescription description)
        {
            _accountName = description.Name;
            _client = new DocumentClient(
                new Uri($"https://{_accountName}.documents.azure.com:443/"),
                description.Key);
            _filters = ParseFilters(description.Filters);
        }

        async Task<IEnumerable<IDatabaseGateway>> ICosmosDbGateway.GetDatabasesAsync()
        {
            var query = _client.CreateDatabaseQuery();
            var dbs = await QueryHelper.GetAllResultsAsync(query.AsDocumentQuery());
            var filteredDbs = from db in dbs
                              where _filters.ContainsKey(db.Id)
                              select new DatabaseGateway(_client, this, db.Id, _filters[db.Id]);
            var gateways = filteredDbs.ToArray<IDatabaseGateway>();

            if (gateways.Length != _filters.Count)
            {
                var set = ImmutableSortedSet.Create(gateways.Select(g => g.Name).ToArray());
                var notFound = from f in _filters
                               where !set.Contains(f.Key)
                               select f.Key;

                throw new BackupException($"Database '{notFound.First()}' not found in account '{_accountName}'");
            }

            return gateways;
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
                    g => g.Select(i => i.Collection).ToArray());

                return dictionary;
            }
        }
    }
}