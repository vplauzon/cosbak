using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers.Index
{
    internal struct ScriptIdentifier
    {
        public ScriptIdentifier(string id, long timeStamp)
        {
            Id = id;
            TimeStamp = timeStamp;
        }

        public string Id { get; }

        public long TimeStamp { get; }
    }
}