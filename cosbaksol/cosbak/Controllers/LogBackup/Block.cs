using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers.LogBackup
{
    public class Block
    {
        public Block(string id, long size)
        {
            Id = id;
            Size = size;
        }

        public string Id { get; }

        public long Size { get; }
    }
}