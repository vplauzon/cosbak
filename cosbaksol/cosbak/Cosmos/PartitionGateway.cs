using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json.Linq;

namespace Cosbak.Cosmos
{
    public class PartitionGateway : IPartitionGateway
    {
        #region Inner Types
        private class AsyncQuery : IAsyncStream<DocumentPackage>
        {
            private readonly IDocumentQuery<Document> _query;
            private readonly IEnumerable<string> _partitionPathParts;

            public AsyncQuery(IDocumentQuery<Document> query, IEnumerable<string> partitionPathParts)
            {
                _query = query;
                _partitionPathParts = partitionPathParts;
            }

            bool IAsyncStream<DocumentPackage>.HasMoreResults => _query.HasMoreResults;

            async Task<DocumentPackage[]> IAsyncStream<DocumentPackage>.GetBatchAsync()
            {
                var batch = await _query.ExecuteNextAsync<JObject>();
                var documents = from d in batch
                                select new DocumentPackage(d, _partitionPathParts);

                return documents.ToArray();
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

        string IPartitionGateway.KeyRangeId => _partitionKeyRangeId;

        IAsyncStream<DocumentPackage> IPartitionGateway.GetChangeFeed()
        {
            var query = _client.CreateDocumentChangeFeedQuery(_collectionUri, new ChangeFeedOptions
            {
                PartitionKeyRangeId = _partitionKeyRangeId,
                StartFromBeginning = true
            });

            return new AsyncQuery(query, _parent.PartitionPath.Split('/').Skip(1));
        }
    }
}