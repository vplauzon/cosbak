using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cosbak.Cosmos
{
    public class DocumentMetaData
    {
        #region Inner Types
        private enum PartitionKeyType
        {
            String,
            Int,
            Float,
            Boolean,
            Null
        }
        #endregion

        public DocumentMetaData(string id, object partitionKey, Int64 timeStamp)
        {
            Id = id;
            PartitionKey = partitionKey;
            TimeStamp = timeStamp;
        }

        public string Id { get; }

        public object PartitionKey { get; }

        public Int64 TimeStamp { get; }

        public void WriteAsync(Stream contentStream)
        {
            using (var writer = new BinaryWriter(contentStream, Encoding.ASCII, true))
            {
                writer.Write(Id);
                WritePartitionKey(writer);
                writer.Write(TimeStamp);
            }
        }

        private void WritePartitionKey(BinaryWriter writer)
        {
            var type = GetPartitionKeyType();

            writer.Write((byte)type);
            switch (type)
            {
                case PartitionKeyType.Boolean:
                    writer.Write((bool)PartitionKey);
                    return;
                case PartitionKeyType.Float:
                    writer.Write((float)PartitionKey);
                    return;
                case PartitionKeyType.Int:
                    writer.Write((int)PartitionKey);
                    return;
                case PartitionKeyType.String:
                    writer.Write((string)PartitionKey);
                    return;
            }
        }

        private PartitionKeyType GetPartitionKeyType()
        {
            if (PartitionKey == null)
            {
                return PartitionKeyType.Null;
            }
            else if (PartitionKey is string)
            {
                return PartitionKeyType.String;
            }
            else if (PartitionKey is int)
            {
                return PartitionKeyType.Int;
            }
            else if (PartitionKey is float)
            {
                return PartitionKeyType.Float;
            }
            else if (PartitionKey is bool)
            {
                return PartitionKeyType.Boolean;
            }
            else
            {
                throw new NotSupportedException(
                    $"Partition key of type '{PartitionKey.GetType().Name}' isn't supported");
            }
        }
    }
}