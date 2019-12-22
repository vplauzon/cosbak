using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Storage
{
    internal class BlockItem
    {
        public BlockItem(string id, long length)
        {
            Id = id;
            Length = length;
        }

        public string Id { get; }

        public long Length { get; }
    }
}