using System;
using System.Linq;

namespace Cosbak.Config
{
    public class StorageAccountDescription
    {
        public string Name { get; set; }

        public string Container { get; set; }

        public string Folder { get; set; }

        public string Key { get; set; }

        public string Token { get; set; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new CosbakException("Storage Account name is required");
            }
            if (string.IsNullOrWhiteSpace(Container))
            {
                throw new CosbakException("Storage Account container is required");
            }
            if (string.IsNullOrWhiteSpace(Key) && string.IsNullOrWhiteSpace(Token))
            {
                throw new CosbakException("Storage Account key or token is required");
            }
        }
    }
}