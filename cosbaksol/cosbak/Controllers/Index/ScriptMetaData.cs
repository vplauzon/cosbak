using System;
using System.IO;
using System.Text;

namespace Cosbak.Controllers.Index
{
    internal class ScriptMetaData : IMetaData
    {
        private readonly int _contentSize;

        public ScriptMetaData(ScriptIdentifier id, int contentSize)
        {
            Id = id;
            _contentSize = contentSize;
        }

        public ScriptIdentifier Id { get; }

        string IMetaData.Id => Id.Id;
        
        int IMetaData.IndexSize => 2 + 4 + 2;

        int IMetaData.ContentSize => _contentSize;

        long IMetaData.TimeStamp => Id.TimeStamp;

        void IMetaData.Write(Stream stream)
        {
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                writer.Write(Id.Id);
                writer.Write(Id.TimeStamp);
                writer.Write(_contentSize);
            }
        }
    }
}