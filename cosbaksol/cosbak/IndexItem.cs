using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cosbak
{
    public class IndexItem : DocumentMetaData
    {
        public IndexItem(string id, object partitionKey, Int64 timeStamp, int size)
            : base(id, partitionKey, timeStamp, size)
        {
            Offset = Offset;
        }

        public long Offset { get; }

        public static new IndexItem Read(BinaryReader reader)
        {
            throw new NotImplementedException();
        }

        public override void Write(BinaryWriter writer)
        {
            base.Write(writer);

            writer.Write(Offset);
        }
    }
}