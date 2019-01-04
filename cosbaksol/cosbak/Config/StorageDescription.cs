using System;

namespace Cosbak.Config
{
    public class StorageDescription
    {
        public string Name { get; set; }

        public string Container { get; set; }

        public string Prefix { get; set; }

        public string Token { get; set; }

        internal void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new BackupException("Storage description must have a name");
            }
            if (string.IsNullOrWhiteSpace(Container))
            {
                throw new BackupException("Storage description must have an existing container");
            }
            if (string.IsNullOrWhiteSpace(Token))
            {
                throw new BackupException("Storage description must have a token");
            }
            if (Token[0] != '?')
            {
                throw new BackupException("Storage description's token must start with a '?'");
            }
        }
    }
}