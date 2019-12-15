using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Storage
{
    internal class BlockItem
    {
        public BlockItem(string name, long length)
        {
            Name = name;
            Length = length;
        }

        public string Name { get; }

        public long Length { get; }
    }
}