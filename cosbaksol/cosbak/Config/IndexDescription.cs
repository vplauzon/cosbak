using System;
using System.Collections.Generic;
using System.Linq;

namespace Cosbak.Config
{
    public class IndexDescription
    {
        public StorageAccountDescription StorageAccount { get; set; }

        public string[] Filters { get; set; }

        public void Validate()
        {
            if (StorageAccount == null)
            {
                throw new CosbakException("Backup Description must contain Storage account");
            }
            StorageAccount.Validate();
            FilterHelper.ValidateFilters(Filters);
        }
    }
}