using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json.Serialization;

namespace Cosbak.Controllers.Index
{
    public struct DocumentMetaData : IMetaData
    {
        private readonly string _id;
        private readonly long _timeStamp;
        private readonly int _contentSize;

        public DocumentMetaData(string id, object? partitionKey, long timeStamp, int contentSize)
        {
            _id = id;
            PartitionHash = partitionKey == null
                ? 0
                : partitionKey.GetHashCode();
            _timeStamp = timeStamp;
            _contentSize = contentSize;
        }

        private DocumentMetaData(string id, int partitionHash, long timeStamp, int contentSize)
        {
            _id = id;
            PartitionHash = partitionHash;
            _timeStamp = timeStamp;
            _contentSize = contentSize;
        }

        public int PartitionHash { get; }

        [JsonIgnore]
        public long CompoundHash
        {
            get
            {
                long longId = _id.GetHashCode();
                long longPartition = PartitionHash;
                long pushedPartition = longPartition << 32;
                long compoundHash = pushedPartition | longId;

                return compoundHash;
            }
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

        string IMetaData.Id => _id;

        int IMetaData.IndexSize => (_id.Length+1) + 4 + 8 + 4;

        int IMetaData.ContentSize => _contentSize;
        
        long IMetaData.TimeStamp => _timeStamp;

        void IMetaData.Write(Stream stream)
        {
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                writer.Write(_id);
                writer.Write(PartitionHash);
                writer.Write(_timeStamp);
                writer.Write(_contentSize);
            }
        }
    }
}