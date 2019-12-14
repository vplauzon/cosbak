using System;

namespace Cosbak.Config
{
    public class OverrideBackupOptions
    {
        public bool? ExplicitDelete { get; set; }

        public bool? TtlDelete { get; set; }

        public bool? AmbiantTtlDelete { get; set; }

        public bool? Sprocs { get; set; }

        public bool? Functions { get; set; }

        public bool? Triggers { get; set; }

        public void Validate()
        {
        }
    }
}