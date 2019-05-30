using System;
using System.Collections.Generic;
using System.IO;

namespace Cosbak
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

        public static DocumentMetaData Read(BinaryReader reader)
        {
            throw new NotImplementedException();
        }

        public virtual void Write(BinaryWriter writer)
        {
            writer.Write(Id);
            WritePartitionKey(writer);
            writer.Write(TimeStamp);
            writer.Write(Size);
        }

        private void WritePartitionKey(BinaryWriter writer)
        {
            if (PartitionKey == null)
            {
                writer.Write((byte)PartitionKeyType.Null);
            }
            else if (PartitionKey is string)
            {
                writer.Write((byte)PartitionKeyType.String);
                writer.Write((string)PartitionKey);
            }
            else if (PartitionKey is Int64)
            {
                writer.Write((byte)PartitionKeyType.Int64);
                writer.Write((Int64)PartitionKey);
            }
            else if (PartitionKey is double)
            {
                writer.Write((byte)PartitionKeyType.Double);
                writer.Write((double)PartitionKey);
            }
            else if (PartitionKey is bool)
            {
                writer.Write((byte)PartitionKeyType.Boolean);
                writer.Write((bool)PartitionKey);
            }
            else
            {
                throw new NotSupportedException(
                    $"Partition key of type '{PartitionKey.GetType().Name}' isn't supported");
            }
        }
    }
}