using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cosbak.Controllers
{
    public class Block
    {
        public string Id { get; set; } = string.Empty;

        public long Size { get; set; }

        internal static (long start, long end) GetInterval(
            IEnumerable<Block> blocks,
            IEnumerable<BlockItem> blobBlocks)
        {
            long start = 0;
            long index = 0;
            var isStarted = false;
            var blockIds = from b in blocks
                           select b.Id;

            foreach (var blobBlock in blobBlocks)
            {
                if (blobBlock.Id == blockIds.First())
                {
                    if (!isStarted)
                    {
                        isStarted = true;
                        start = index;
                    }
                    else
                    {
                    }
                    blockIds = blockIds.Skip(1);
                    if (!blockIds.Any())
                    {
                        return (start, index + blobBlock.Length);
                    }
                }
                else
                {
                    if (!isStarted)
                    {
                    }
                    else
                    {
                        throw new NotSupportedException("Blocks aren't contiguous");
                    }
                }
                index += blobBlock.Length;
            }

            throw new NotSupportedException("Remaining blocks that aren't in the blob");
        }
    }
}