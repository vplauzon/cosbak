using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json.Linq;

namespace Cosbak.Cosmos
{
    public class PartitionFacade : IPartitionFacade
    {
        #region Inner Types
        private class AsyncQuery : IAsyncStream<JObject>
        {
            private readonly IDocumentQuery<Document> _query;

            public AsyncQuery(IDocumentQuery<Document> query)
            {
                _query = query;
            }

            bool IAsyncStream<JObject>.HasMoreResults => _query.HasMoreResults;

            async Task<IImmutableList<JObject>> IAsyncStream<JObject>.GetBatchAsync()
            {
                var batch = await _query.ExecuteNextAsync<JObject>();

                return batch.ToImmutableArray();
            }
        }
        #endregion

        private readonly DocumentClient _client;
        private readonly ICollectionFacade _parent;
        private readonly string _partitionKeyRangeId;
        private readonly Uri _collectionUri;

        public PartitionFacade(DocumentClient client, ICollectionFacade parent, string partitionKeyRangeId)
        {
            _client = client;
            _parent = parent;
            _partitionKeyRangeId = partitionKeyRangeId;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(
                _parent.Parent.DatabaseName,
                _parent.CollectionName);
        }

        string IPartitionFacade.KeyRangeId => _partitionKeyRangeId;

        IAsyncStream<JObject> IPartitionFacade.GetChangeFeed()
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