using System;
using System.Collections.Generic;
using System.Linq;

namespace Cosbak.Config
{
    public class IndexDescription
    {
        public CosmosAccountDescription CosmosAccount { get; set; }

        public StorageAccountDescription StorageAccount { get; set; }

        public string[] Filters { get; set; }

        public void Validate()
        {
            if (CosmosAccount == null)
            {
                throw new CosbakException("Backup Description must contain Cosmos DB account");
            }
            CosmosAccount.ValidateNameOnly();
            if (StorageAccount == null)
            {
                throw new CosbakException("Backup Description must contain Storage account");
            }
            StorageAccount.Validate();
            FilterHelper.ValidateFilters(Filters);
        }
    }
}