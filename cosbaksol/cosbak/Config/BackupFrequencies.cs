using System;

namespace Cosbak.Config
{
    public class BackupFrequencies
    {
        public TimeSpan? Content { get; set; }

        public TimeSpan? Config { get; set; }

        public TimeSpan? Deleted { get; set; }

        public TimeSpan? Sprocs { get; set; }

        public TimeSpan? Functions { get; set; }

        public TimeSpan? Triggers { get; set; }
    }
}