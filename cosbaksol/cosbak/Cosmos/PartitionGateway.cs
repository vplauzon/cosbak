using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Cosbak.Cosmos
{
    public class PartitionGateway : IPartitionGateway
    {
        #region Inner Types
        private class AsyncQuery : IAsyncStream<IDictionary<string, object>>
        {
            private readonly IDocumentQuery<Document> _query;

            public AsyncQuery(IDocumentQuery<Document> query)
            {
                _query = query;
            }

            bool IAsyncStream<IDictionary<string, object>>.HasMoreResults => _query.HasMoreResults;

            async Task<IDictionary<string, object>[]> IAsyncStream<IDictionary<string, object>>.GetBatchAsync()
            {
                var batch = await _query.ExecuteNextAsync<IDictionary<string, object>>();
                var batchResult = batch.ToArray();

                return batchResult;
            }
        }
        #endregion

        private readonly DocumentClient _client;
        private readonly ICollectionGateway _parent;
        private readonly string _partitionKeyRangeId;
        private readonly Uri _collectionUri;

        public PartitionGateway(DocumentClient client, ICollectionGateway parent, string partitionKeyRangeId)
        {
            _client = client;
            _parent = parent;
            _partitionKeyRangeId = partitionKeyRangeId;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(_parent.Parent.DatabaseName, _parent.CollectionName);
        }

        IAsyncStream<IDictionary<string, object>> IPartitionGateway.GetChangeFeed()
        {
            var query = _client.CreateDocumentChangeFeedQuery(_collectionUri, new ChangeFeedOptions
            {
                PartitionKeyRangeId = _partitionKeyRangeId,
                StartFromBeginning = true
            });

            return new AsyncQuery(query);
        }
    }
}