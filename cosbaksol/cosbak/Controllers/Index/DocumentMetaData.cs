using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cosbak.Controllers.Index
{
    public struct DocumentMetaData
    {
        public DocumentMetaData(string id, object? partitionKey, long timeStamp, int size)
        {
            Id = id;
            PartitionHash = partitionKey == null
                ? 0
                : partitionKey.GetHashCode();
            TimeStamp = timeStamp;
            Size = size;
        }

        private DocumentMetaData(string id, int partitionHash, long timeStamp, int size)
        {
            Id = id;
            PartitionHash = partitionHash;
            TimeStamp = timeStamp;
            Size = size;
        }

        public string Id { get; }

        public int PartitionHash { get; }

        public long TimeStamp { get; }

        public int Size { get; }

        public long GetCompoundHash()
        {
            long longId = Id.GetHashCode();
            long longPartition = PartitionHash;
            long pushedPartition = longPartition << 32;
            long compoundHash = pushedPartition | longId;

            return compoundHash;
        }

        public static DocumentMetaData Read(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                var id = reader.ReadString();
                var partitionHash = reader.ReadInt32();
                var timeStamp = reader.ReadInt64();
                var size = reader.ReadInt32();

                return new DocumentMetaData(id, partitionHash, timeStamp, size);
            }
        }

        public void Write(Stream stream)
        {
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                writer.Write(Id);
                writer.Write(PartitionHash);
                writer.Write(TimeStamp);
                writer.Write(Size);
            }
        }
    }
}