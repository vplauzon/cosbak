using System;
using System.Collections.Generic;
using System.Linq;

namespace Cosbak.Controllers.Backup
{
    public class BackupConfiguration
    {
        public CosmosAccountDescription CosmosAccount { get; set; } = new CosmosAccountDescription();

        public StorageAccountDescription StorageAccount { get; set; } = new StorageAccountDescription();

        public void Validate()
        {
            CosmosAccount.Validate();
            StorageAccount.Validate();
        }
    }
}