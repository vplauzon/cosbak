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
        private class AsyncQuery : IAsyncStream<DocumentObject>
        {
            private readonly IDocumentQuery<Document> _query;

            public AsyncQuery(IDocumentQuery<Document> query)
            {
                _query = query;
            }

            bool IAsyncStream<DocumentObject>.HasMoreResults => _query.HasMoreResults;

            async Task<DocumentObject[]> IAsyncStream<DocumentObject>.GetBatchAsync()
            {
                var batch = await _query.ExecuteNextAsync<IDictionary<string, object>>();
                var documents = (from d in batch
                                 let metaData = ExtractMetaData(d)
                                 select new DocumentObject(metaData, d)).ToArray();

                foreach (var doc in documents)
                {
                    Clean(doc.Content);
                }

                return documents.ToArray();
            }

            private DocumentMetaData ExtractMetaData(IDictionary<string, object> document)
            {
                return new DocumentMetaData((string)document["id"], "TO DO", (Int64)document["_ts"]);
            }

            private void Clean(IDictionary<string, object> content)
            {
                content.Remove("id");
                content.Remove("_ts");
                content.Remove("_rid");
                content.Remove("_self");
                content.Remove("_etag");
                content.Remove("_attachments");
                content.Remove("_lsn");
                content.Remove("_metadata");
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

        IAsyncStream<DocumentObject> IPartitionGateway.GetChangeFeed()
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