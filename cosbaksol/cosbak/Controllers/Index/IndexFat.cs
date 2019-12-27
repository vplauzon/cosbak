using System;
using System.Collections.Generic;
using System.Linq;

namespace Cosbak.Controllers.Index
{
    internal class IndexFat
    {
        public long LastDocumentTimeStamp { get; set; }
        
        public long LastStoredProcedureTimeStamp { get; set; }

        public IndexedPartition DocumentPartition { get; set; } = new IndexedPartition();
        
        public IndexedPartition SprocPartition { get; set; } = new IndexedPartition();

        public IEnumerable<Block> GetAllBlocks()
        {
            return DocumentPartition.GetAllBlocks()
                .Concat(SprocPartition.GetAllBlocks());
        }
    }
}