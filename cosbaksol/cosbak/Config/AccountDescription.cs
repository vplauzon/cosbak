using System;
using System.Linq;

namespace cosbak.Config
{
    public class AccountDescription
    {
        public string Name { get; set; }

        public string Key { get; set; }

        public string[] Filters { get; set; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new BackupException("Account description must have a name");
            }
            if (string.IsNullOrWhiteSpace(Key))
            {
                throw new BackupException("Account description must have a key");
            }
            if (Filters != null)
            {
                if (Filters.Any(s => string.IsNullOrWhiteSpace(s)))
                {
                    throw new BackupException("Account description filters are optional ; if present they must be non-empty");
                }
            }
        }
    }
}