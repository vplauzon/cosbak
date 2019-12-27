using System;
using System.Drawing;
using System.IO;
using System.Text;

namespace Cosbak.Controllers.Index
{
    internal class ScriptMetaData : IStreamable
    {
        public ScriptMetaData(ScriptIdentifier id, int contentSize)
        {
            Id = id;
            ContentSize = contentSize;
        }

        public ScriptIdentifier Id { get; }

        public int ContentSize { get; }

        int IStreamable.Size => 2 + 4 + 2;

        void IStreamable.Write(Stream stream)
        {
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                writer.Write(Id.Id);
                writer.Write(Id.TimeStamp);
                writer.Write(ContentSize);
            }
        }
    }
}