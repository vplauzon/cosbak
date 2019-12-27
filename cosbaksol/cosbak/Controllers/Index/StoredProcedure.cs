using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Unicode;

namespace Cosbak.Controllers.Index
{
    internal class StoredProcedure
    {
        public StoredProcedure(
            string id,
            long timeStamp,
            string body)
        {
            Id = new ScriptIdentifier(id, timeStamp);
            Body = body;
        }

        public ScriptIdentifier Id { get; }

        public string Body { get; }

        public (ScriptMetaData meta, byte[] content) Split()
        {
            return (
                new ScriptMetaData(Id, Body.Length),
                ASCIIEncoding.UTF8.GetBytes(Body));
        }
    }
}