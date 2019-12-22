using System;
using System.Collections.Generic;

namespace Cosbak.Controllers.Index
{
    internal class IndexFat
    {
        public long LastDocumentTimeStamp { get; set; }

        public IndexedPartition DocumentPartition { get; set; } = new IndexedPartition();

        public IEnumerable<Block> GetAllBlocks()
        {
            return DocumentPartition.GetAllBlocks();
        }
    }
}