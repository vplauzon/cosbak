using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Cosbak.Cosmos
{
    internal class CollectionGateway : ICollectionGateway
    {
        private readonly DocumentClient _client;
        private readonly string _collectionName;
        private readonly string _partitionPath;
        private readonly IDatabaseGateway _parent;
        private readonly Uri _collectionUri;

        public CollectionGateway(DocumentClient client, string collectionName, string partitionPath, IDatabaseGateway parent)
        {
            _client = client;
            _collectionName = collectionName;
            _partitionPath = partitionPath;
            _parent = parent;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(_parent.DatabaseName, _collectionName);
        }

        IDatabaseGateway ICollectionGateway.Parent => _parent;

        string ICollectionGateway.CollectionName => _collectionName;

        string ICollectionGateway.PartitionPath => _partitionPath;

        async Task<long> ICollectionGateway.GetLastUpdateTimeAsync()
        {
            var sql = new SqlQuerySpec("SELECT TOP 1 c._ts FROM c ORDER BY c._ts DESC");
            var query = _client.CreateDocumentQuery<IDictionary<string, long>>(_collectionUri, sql, new FeedOptions
            {
                EnableCrossPartitionQuery = true
            });
            var results = await QueryHelper.GetAllResultsAsync(query.AsDocumentQuery());
            var time = results[0].First().Value;

            return time;
        }

        async Task<IPartitionGateway[]> ICollectionGateway.GetPartitionsAsync()
        {
            var keyRanges = await _client.ReadPartitionKeyRangeFeedAsync(_collectionUri);
            var gateways = from k in keyRanges
                           select new PartitionGateway(_client, this, k.Id);

            return gateways.ToArray();
        }
    }
}