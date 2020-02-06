using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace Cosbak.Controllers.Index
{
    internal class IndexedPartition
    {
        public IImmutableList<Block> IndexBlocks { get; set; } = ImmutableList<Block>.Empty;

        public IImmutableList<Block> ContentBlocks { get; set; } = ImmutableList<Block>.Empty;

        public IndexedPartition AddBlocks(Block indexBlock, Block contentBlock)
        {
            return new IndexedPartition
            {
                IndexBlocks = IndexBlocks.Add(indexBlock),
                ContentBlocks = ContentBlocks.Add(contentBlock)
            };
        }

        public IEnumerable<Block> GetAllBlocks()
        {
            return IndexBlocks
                .Concat(ContentBlocks);
        }
    }
}