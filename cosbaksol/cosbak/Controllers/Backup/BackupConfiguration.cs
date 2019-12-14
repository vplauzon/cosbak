using System;
using System.Collections.Generic;
using System.Linq;

namespace Cosbak.Controllers.Backup
{
    public class BackupConfiguration
    {
        public CosmosAccountConfiguration CosmosAccount { get; set; } = new CosmosAccountConfiguration();

        public StorageAccountConfiguration StorageAccount { get; set; } = new StorageAccountConfiguration();

        public void Validate()
        {
            CosmosAccount.Validate();
            StorageAccount.Validate();
        }
    }
}