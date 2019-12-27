using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers.Index
{
    internal abstract class LogItemBatch<T>
    {
        protected LogItemBatch(long batchTimeStamp, IEnumerable<T> items)
        {
            BatchTimeStamp = batchTimeStamp;
            Items = items;
        }

        public long BatchTimeStamp { get; }
        
        public IEnumerable<T> Items { get; }
    }
}