using System;
using System.Linq;

namespace Cosbak.Config
{
    public class CosmosAccountDescription
    {
        public string Name { get; set; }

        public string Key { get; set; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new CosbakException("Cosmos Account name is required");
            }
            if (string.IsNullOrWhiteSpace(Key))
            {
                throw new CosbakException("Cosmos Account key is required");
            }
        }
    }
}