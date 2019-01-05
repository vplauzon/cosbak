using System;

namespace Cosbak.Config
{
    public class StorageDescription
    {
        public string Name { get; set; }

        public string Container { get; set; }

        public string Prefix { get; set; }

        public string Key { get; set; }

        public string Token { get; set; }

        internal void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new BackupException("Storage account name is required");
            }
            if (string.IsNullOrWhiteSpace(Container))
            {
                throw new BackupException("Storage container is required");
            }
            if (string.IsNullOrWhiteSpace(Key) && string.IsNullOrWhiteSpace(Token))
            {
                throw new BackupException("Storage token or key is required");
            }
            if (!string.IsNullOrWhiteSpace(Token) && Token[0] != '?')
            {
                throw new BackupException("Storage token must start with a '?'");
            }
        }
    }
}