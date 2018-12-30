using System;
using System.Collections.Generic;
using System.Text;

namespace cosbak.Config
{
    public class BackupDescription
    {
        public AccountDescription[] Accounts { get; set; }

        public StorageDescription Storage { get; set; }

        public RamDescription Ram { get; set; }
    }
}