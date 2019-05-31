using System;
using System.Collections.Generic;
using System.IO;

namespace Cosbak
{
    public struct DocumentMetaData
    {
        public DocumentMetaData(string id, object partitionKey, long timeStamp, int size)
        {
            Id = id;
            Hash = new DocumentHash(id, partitionKey);
            TimeStamp = timeStamp;
            Size = size;
        }

        private DocumentMetaData(string id, DocumentHash hash, long timeStamp, int size)
        {
            Id = id;
            Hash = hash;
            TimeStamp = timeStamp;
            Size = size;
        }

        public string Id { get; }

        public DocumentHash Hash { get; }

        public long TimeStamp { get; }

        public int Size { get; }

        public static DocumentMetaData Read(BinaryReader reader)
        {
            var id = reader.ReadString();
            var hash = DocumentHash.Read(reader);
            var timeStamp = reader.ReadInt64();
            var size = reader.ReadInt32();

            return new DocumentMetaData(id, hash, timeStamp, size);
        }

        public void Write(BinaryWriter writer)
        {
            writer.Write(Id);
            Hash.Write(writer);
            writer.Write(TimeStamp);
            writer.Write(Size);
        }
    }
}