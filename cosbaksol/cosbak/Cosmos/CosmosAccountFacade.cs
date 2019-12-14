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

        async Task<ICollectionFacade> ICosmosAccountFacade.GetCollectionAsync(
            string db,
            string collection)
        {
            var queryDb = from d in _client.CreateDatabaseQuery()
                          where d.Id == db
                          select d;
            var dbs = await QueryHelper.GetAllResultsAsync(queryDb.AsDocumentQuery());

            if (dbs.Length == 0)
            {
                throw new CosbakException($"Db '{db}' doesn't exist");
            }
            else
            {
                var queryCollection =
                    from c in _client.CreateDocumentCollectionQuery(dbs.First().SelfLink)
                    where c.Id == collection
                    select c;
                var collections = await QueryHelper.GetAllResultsAsync(queryCollection.AsDocumentQuery());

                if (collections.Length == 0)
                {
                    throw new CosbakException($"Collection '{collection}' doesn't exist");
                }
                else
                {
                    return new CollectionFacade(
                        _client,
                        this,
                        db,
                        collection,
                        collections.First().PartitionKey.Paths.First());
                }
            }
        }
    }
}