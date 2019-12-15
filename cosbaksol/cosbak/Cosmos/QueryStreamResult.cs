using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Text;

namespace Cosbak.Cosmos
{
    public class QueryStreamResult
    {
        public QueryStreamResult(Stream stream, double requestCharge)
        {
            Stream = stream;
            RequestCharge = requestCharge;
        }

        public Stream Stream { get; }

        public double RequestCharge { get; }
    }
}