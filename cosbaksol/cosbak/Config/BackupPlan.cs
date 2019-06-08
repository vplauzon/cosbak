using System;
using System.Collections.Immutable;
using System.Linq;

namespace Cosbak.Config
{
    public class BackupPlan
    {
        public TimeSpan? Content { get; set; }

        public TimeSpan? Config { get; set; }

        public TimeSpan? Deleted { get; set; }

        public TimeSpan? Sprocs { get; set; }

        public TimeSpan? Functions { get; set; }

        public TimeSpan? Triggers { get; set; }

        public void Validate()
        {
        }
    }
}