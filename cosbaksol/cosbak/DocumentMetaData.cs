using System;
using System.Collections.Generic;
using System.IO;

namespace Cosbak.Cosmos
{
    public class DocumentMetaData
    {
        #region Inner Types
        private enum PartitionKeyType
        {
            String,
            Int64,
            Double,
            Boolean,
            Null
        }
        #endregion

        public DocumentMetaData(string id, object partitionKey, Int64 timeStamp, int size)
        {
            Id = id;
            PartitionKey = partitionKey;
            TimeStamp = timeStamp;
            Size = size;
        }

        public string Id { get; }

        public object PartitionKey { get; }

        public Int64 TimeStamp { get; }

        public int Size { get; }

        public void WriteAsync(BinaryWriter writer)
        {
            writer.Write(Id);
            WritePartitionKey(writer);
            writer.Write(TimeStamp);
            writer.Write(Size);
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
                case PartitionKeyType.Double:
                    writer.Write((double)PartitionKey);
                    return;
                case PartitionKeyType.Int64:
                    writer.Write((Int64)PartitionKey);
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
            else if (PartitionKey is Int64)
            {
                return PartitionKeyType.Int64;
            }
            else if (PartitionKey is double)
            {
                return PartitionKeyType.Double;
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