using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

namespace Cosbak.Cosmos
{
    internal class QueryItemResult<T>
    {
        public QueryItemResult(IImmutableList<T> content, double requestCharge)
        {
            Content = content;
            RequestCharge = requestCharge;
        }

        public IImmutableList<T> Content { get; }

        public double RequestCharge { get; }
    }
}