using System;
using System.Collections.Generic;
using System.Linq;

namespace Cosbak.Controllers.Backup
{
    public class BackupConfiguration
    {
        public CosmosAccountConfiguration CosmosAccount { get; set; } = new CosmosAccountConfiguration();

        public StorageAccountConfiguration StorageAccount { get; set; } = new StorageAccountConfiguration();
        
        public BackupPlan GeneralPlan { get; set; } = new BackupPlan();

        public CollectionBackupPlan[] Collections { get; set; } = new CollectionBackupPlan[0];

        public void Validate()
        {
            CosmosAccount.Validate();
            StorageAccount.Validate();
            GeneralPlan.Validate();
            if (Collections.Length == 0)
            {
                throw new CosbakException("No collection defined");
            }
            foreach (var collection in Collections)
            {
                collection.Validate();
            }
        }
    }
}