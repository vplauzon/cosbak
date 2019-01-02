using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Cosmos
{
    public class DocumentMetaData
    {
        public DocumentMetaData(string id, object partitionKey, Int64 timeStamp)
        {
            Id = id;
            PartitionKey = partitionKey;
            TimeStamp = timeStamp;
        }

        public string Id { get; }

        public object PartitionKey { get; }

        public Int64 TimeStamp { get; }
    }
}