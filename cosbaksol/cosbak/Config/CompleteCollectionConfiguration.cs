using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Config
{
    public class CompleteCollectionConfiguration : CollectionConfiguration
    {
        public string Account { get; set; } = string.Empty;

        public new void Validate()
        {
            base.Validate();

            if (string.IsNullOrWhiteSpace(Account))
            {
                throw new CosbakException("Cosmos Account name is required");
            }
        }
    }
}