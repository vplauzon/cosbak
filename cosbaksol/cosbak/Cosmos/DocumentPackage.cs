using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Cosbak.Cosmos
{
    public class DocumentPackage
    {
        #region Inner Types
        private struct BasicMetaData
        {
            public string Id { get; set; }

            public object PartitionKey { get; set; }

            public Int64 TimeStamp { get; set; }
        }
        #endregion

        public DocumentPackage(JObject document, IEnumerable<string> partitionPathParts)
        {
            var basics = ExtractMetaData(document, partitionPathParts);

            Clean(document, partitionPathParts);

            Content = Serialize(document);
            MetaData = new DocumentMetaData(basics.Id, basics.PartitionKey, basics.TimeStamp, Content.Length);
        }

        public DocumentMetaData MetaData { get; }

        public byte[] Content { get; }

        private BasicMetaData ExtractMetaData(JObject document, IEnumerable<string> partitionPathParts)
        {
            var partition = ExtractPartition(document, partitionPathParts);

            return new BasicMetaData
            {
                Id = document["id"].Value<string>(),
                PartitionKey = partition,
                TimeStamp = document["_ts"].Value<Int64>()
            };
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

        private void Clean(JObject content, IEnumerable<string> partitionPathParts)
        {
            RemovePartition(content, partitionPathParts);
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

        private static byte[] Serialize(JObject document)
        {
            //  Replace by BsonWriter to serialize in BSON
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var jsonWriter = new JsonTextWriter(writer))
            {
                document.WriteTo(jsonWriter);
                jsonWriter.Flush();
                writer.Flush();
                stream.Flush();

                return stream.ToArray();
            }
        }
    }
}