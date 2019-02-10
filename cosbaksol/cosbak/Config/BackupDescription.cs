using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cosbak.Config
{
    public class BackupDescription
    {
        public CosmosAccountDescription CosmosAccount { get; set; }

        public AppInsightsDescription AppInsights { get; set; }

        public BackupPlan Plan { get; set; }

        public void Validate()
        {
            if (CosmosAccount == null)
            {
                throw new BackupException("Backup Description must contain Cosmos DB account");
            }
            CosmosAccount.Validate();
            if (AppInsights != null)
            {
                AppInsights.Validate();
            }
            if (Plan == null)
            {
                throw new BackupException("Backup Description must contain a plan");
            }
            Plan.Validate();
        }
    }
}