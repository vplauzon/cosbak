using System;

namespace Cosbak.Config
{
    public class CollectionConfiguration
    {
        public string Db { get; set; } = string.Empty;
        
        public string Collection { get; set; } = string.Empty;

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Db))
            {
                throw new CosbakException("Cosmos Database name is required");
            }
            if (string.IsNullOrWhiteSpace(Collection))
            {
                throw new CosbakException("Cosmos Collection name is required");
            }
        }
    }
}