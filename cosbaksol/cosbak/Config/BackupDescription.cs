using System;
using System.Collections.Generic;
using System.Linq;

namespace Cosbak.Config
{
    public class BackupDescription
    {
        public CosmosAccountDescription CosmosAccount { get; set; }

        public StorageAccountDescription StorageAccount { get; set; }

        public string[] Filters { get; set; }

        public BackupPlan Plan { get; set; }

        public void Validate()
        {
            if (CosmosAccount == null)
            {
                throw new CosbakException("Backup Description must contain Cosmos DB account");
            }
            CosmosAccount.Validate();
            if (StorageAccount == null)
            {
                throw new CosbakException("Backup Description must contain Storage account");
            }
            StorageAccount.Validate();
            FilterHelper.ValidateFilters(Filters);
            if (Plan != null)
            {
                throw new CosbakException("Backup Description must contain a plan");
            }
            Plan.Validate();
        }
    }
}