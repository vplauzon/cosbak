using System;
using System.Linq;

namespace Cosbak.Config
{
    public class CosmosAccountDescription
    {
        public string Name { get; set; }

        public string Key { get; set; }

        public void ValidateNameOnly()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new CosbakException("Cosmos Account name is required");
            }
        }

        public void ValidateFull()
        {
            ValidateNameOnly();
            if (string.IsNullOrWhiteSpace(Key))
            {
                throw new CosbakException("Cosmos Account key is required");
            }
        }
    }
}