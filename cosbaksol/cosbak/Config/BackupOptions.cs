using System;

namespace Cosbak.Config
{
    public class BackupOptions
    {
        public bool ExplicitDelete { get; set; } = true;
      
        public bool TtlDelete { get; set; } = true;
     
        public bool AmbiantTtlDelete { get; set; } = true;
    
        public bool Sprocs { get; set; } = true;
    
        public bool Functions { get; set; } = true;
     
        public bool Triggers { get; set; } = true;

        public void Validate()
        {
        }
    }
}