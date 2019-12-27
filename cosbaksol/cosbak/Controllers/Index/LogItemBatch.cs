using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers.Index
{
    internal class BatchedItems<T>
    {
        public BatchedItems(long batchTimeStamp, IEnumerable<T> items)
        {
            BatchTimeStamp = batchTimeStamp;
            Items = items;
        }

        public long BatchTimeStamp { get; }
        
        public IEnumerable<T> Items { get; }
    }
}