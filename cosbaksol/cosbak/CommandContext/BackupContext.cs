using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.CommandContext
{
    public class BackupContext
    {
        public string File { get; set; }

        public string CosmosAccountName { get; set; }

        public string CosmosAccountKey { get; set; }

        public string CosmosFilter { get; set; }

        public string StorageAccountName { get; set; }

        public string StorageContainer { get; set; }

        public string StoragePrefix { get; set; }

        public string StorageKey { get; set; }

        public string StorageToken { get; set; }

        public string ApplicationInsightsKey { get; set; }

        public string ApplicationInsightsRole { get; set; }
    }
}