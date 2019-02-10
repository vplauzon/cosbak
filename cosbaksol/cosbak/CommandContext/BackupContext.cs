using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.CommandContext
{
    public class BackupContext
    {
        public string FolderUri { get; set; }

        public string CosmosAccountKey { get; set; }

        public string ApplicationInsightsKey { get; set; }
    }
}