using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak
{
    public static class DocumentSpliter
    {
        #region Inner Types
        private struct BasicMetaData
        {
            public string Id { get; set; }

            public object? PartitionKey { get; set; }

            public Int64 TimeStamp { get; set; }
        }
        #endregion

        public static DocumentMetaData WriteContent(
            JObject document,
            IEnumerable<string> partitionPathParts,
            BinaryWriter writer)
        {
            //  Copy so we can manipulate it without side effect
            document = (JObject)document.DeepClone();

            var basics = ExtractMetaData(document, partitionPathParts);
            var positionBefore = writer.BaseStream.Position;

            CleanMetaData(document);

            using (var jsonWriter = new BsonDataWriter(writer))
            {
                document.WriteTo(jsonWriter);
                jsonWriter.Flush();

                var positionAfter = writer.BaseStream.Position;
                var size = positionAfter - positionBefore;

                if (size > int.MaxValue)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(document),
                        $"Document is too big ; size of {size}");
                }

                var metaData = new DocumentMetaData(
                    basics.Id,
                    basics.PartitionKey,
                    basics.TimeStamp,
                    (int)size);

                return metaData;
            }
        }

        public static JObject Read(
            DocumentMetaData metaData,
            IEnumerable<string> partitionPathParts,
            BinaryReader reader)
        {
            var positionBefore = reader.BaseStream.Position;

            using (var jsonReader = new BsonDataReader(reader))
            {
                var document = JObject.Load(jsonReader);
                var positionAfter = reader.BaseStream.Position;
                var size = positionAfter - positionBefore;

                if (size != metaData.Size)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(metaData),
                        "Mismatch between the meta size and the buffer size read in BSON:"
                        + $"{size} vs {metaData.Size}");
                }
                IntegrateMetaData(document, metaData);

                return document;
            }
        }

        private static BasicMetaData ExtractMetaData(JObject document, IEnumerable<string> partitionPathParts)
        {
            var partition = GetPartitionValue(document, partitionPathParts);

            return new BasicMetaData
            {
                Id = document["id"].Value<string>(),
                PartitionKey = partition,
                TimeStamp = document["_ts"].Value<Int64>()
            };
        }

        private static object? GetPartitionValue(JObject document, IEnumerable<string> partitionPathParts)
        {
            var field = partitionPathParts.First();

            if (document.TryGetValue(field, out var fieldObject))
            {
                var trailingFields = partitionPathParts.Skip(1);

                if (trailingFields.Any())
                {
                    var fieldValue = fieldObject.Value<JObject>();

                    return GetPartitionValue(fieldValue, trailingFields);
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

        private static void CleanMetaData(JObject document)
        {
            document.Remove("id");
            document.Remove("_ts");
            document.Remove("_rid");
            document.Remove("_self");
            document.Remove("_etag");
            document.Remove("_attachments");
            document.Remove("_lsn");
            document.Remove("_metadata");
        }

        private static void IntegrateMetaData(JObject document, DocumentMetaData metaData)
        {
            document.Add("id", metaData.Id);
        }
    }
}