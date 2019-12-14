using System;
using System.Linq;

namespace Cosbak.Controllers
{
    public class CosmosAccountConfiguration
    {
        public string Name { get; set; } = string.Empty;

        public string Key { get; set; } = string.Empty;
        
        public string KeyPath { get; set; } = string.Empty;

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new CosbakException("Cosmos Account name is required");
            }
            if (string.IsNullOrWhiteSpace(Key) && string.IsNullOrWhiteSpace(KeyPath))
            {
                throw new CosbakException("Cosmos Account key (or key path) is required");
            }
        }
    }
}