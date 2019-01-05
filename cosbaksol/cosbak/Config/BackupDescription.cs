using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cosbak.Config
{
    public class BackupDescription
    {
        public CosmosAccountDescription[] CosmosAccounts { get; set; }

        public StorageDescription Storage { get; set; }

        public AppInsightsDescription AppInsights { get; set; }

        public void Validate()
        {
            if (CosmosAccounts == null || !CosmosAccounts.Any())
            {
                throw new BackupException("Backup Description must contain at least one Cosmos DB account");
            }
            if (Storage == null)
            {
                throw new BackupException("Backup Description must contain storage description");
            }
            foreach (var a in CosmosAccounts)
            {
                a.Validate();
            }
            Storage.Validate();
            if (AppInsights != null)
            {
                AppInsights.Validate();
            }
        }
    }
}