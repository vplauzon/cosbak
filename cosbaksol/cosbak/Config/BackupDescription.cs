using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cosbak.Config
{
    public class BackupDescription
    {
        public AccountDescription[] Accounts { get; set; }

        public StorageDescription Storage { get; set; }

        public RamDescription Ram { get; set; }

        public AppInsightsDescription AppInsights { get; set; }

        public void Validate()
        {
            if (Accounts == null || !Accounts.Any())
            {
                throw new BackupException("Backup Description must contain at least one Cosmos DB account");
            }
            if (Storage == null)
            {
                throw new BackupException("Backup Description must contain storage description");
            }
            foreach (var a in Accounts)
            {
                a.Validate();
            }
            Storage.Validate();
            if (Ram != null)
            {
                Ram.Validate();
            }
            if (AppInsights != null)
            {
                AppInsights.Validate();
            }
        }
    }
}