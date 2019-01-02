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
        private class AsyncQuery : IAsyncStream<DocumentObject>
        {
            private readonly IDocumentQuery<Document> _query;
            private readonly IEnumerable<string> _partitionPathParts;

            public AsyncQuery(IDocumentQuery<Document> query, IEnumerable<string> partitionPathParts)
            {
                _query = query;
                _partitionPathParts = partitionPathParts;
            }

            bool IAsyncStream<DocumentObject>.HasMoreResults => _query.HasMoreResults;

            async Task<DocumentObject[]> IAsyncStream<DocumentObject>.GetBatchAsync()
            {
                var batch = await _query.ExecuteNextAsync<JObject>();
                var documents = (from d in batch
                                 let metaData = ExtractMetaData(d)
                                 select new DocumentObject(metaData, d)).ToArray();

                foreach (var doc in documents)
                {
                    Clean(doc.Content);
                }

                return documents.ToArray();
            }

            private DocumentMetaData ExtractMetaData(JObject document)
            {
                var partition = ExtractPartition(document, _partitionPathParts);

                return new DocumentMetaData(document["id"].Value<string>(), partition, document["_ts"].Value<Int64>());
            }

            private object ExtractPartition(JObject document, IEnumerable<string> partitionPathParts)
            {
                var field = partitionPathParts.First();

                if (document.TryGetValue(field, out var fieldObject))
                {
                    var trailingFields = partitionPathParts.Skip(1);

                    if (trailingFields.Any())
                    {
                        var fieldValue = fieldObject.Value<JObject>();

                        return ExtractPartition(fieldValue, trailingFields);
                    }
                    else
                    {
                        var fieldValue = fieldObject.Value<JValue>();

                        return fieldValue.Value;
                    }
                }
                else
                {
                    return null;
                }
            }

            private void Clean(JObject content)
            {
                RemovePartition(content, _partitionPathParts);
                content.Remove("id");
                content.Remove("_ts");
                content.Remove("_rid");
                content.Remove("_self");
                content.Remove("_etag");
                content.Remove("_attachments");
                content.Remove("_lsn");
                content.Remove("_metadata");
            }

            private void RemovePartition(JObject document, IEnumerable<string> partitionPathParts)
            {
                var field = partitionPathParts.First();

                if (document.TryGetValue(field, out var fieldValue))
                {
                    var trailingFields = partitionPathParts.Skip(1);

                    if (trailingFields.Any())
                    {
                        RemovePartition(fieldValue.Value<JObject>(), trailingFields);
                    }
                    else
                    {
                        document.Remove(field);
                    }
                }
                else
                {
                    //  Partition key isn't defined
                }
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

            return new AsyncQuery(query, _parent.PartitionPath.Split('/').Skip(1));
        }
    }
}