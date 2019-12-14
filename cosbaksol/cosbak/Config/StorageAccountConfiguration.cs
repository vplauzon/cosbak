using System;
using System.Linq;

namespace Cosbak.Config
{
    public class StorageAccountConfiguration
    {
        public string Name { get; set; } = string.Empty;

        public string Container { get; set; } = string.Empty;

        public string Folder { get; set; } = string.Empty;

        public string Key { get; set; } = string.Empty;
        
        public string KeyPath { get; set; } = string.Empty;

        public string Token { get; set; } = string.Empty;
        
        public string TokenPath { get; set; } = string.Empty;

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
            if (string.IsNullOrWhiteSpace(Key)
                && string.IsNullOrWhiteSpace(KeyPath)
                && string.IsNullOrWhiteSpace(Token)
                && string.IsNullOrWhiteSpace(TokenPath))
            {
                throw new CosbakException(
                    "Storage Account key, key path, token or token path is requires");
            }
        }
    }
}