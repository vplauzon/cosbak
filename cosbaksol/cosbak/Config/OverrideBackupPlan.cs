using System;

namespace Cosbak.Config
{
    public class OverrideBackupPlan
    {
        public int? RetentionInDays { get; set; }

        public int? RpoInSeconds { get; set; }

        public OverrideBackupOptions Included { get; set; } = new OverrideBackupOptions();

        public void Validate()
        {
            if (RetentionInDays != null && RetentionInDays <= 0)
            {
                throw new CosbakException("RetentionInDays must be positive");
            }
            if (RpoInSeconds != null && RpoInSeconds <= 0)
            {
                throw new CosbakException("RpoInSeconds must be positive");
            }
            Included.Validate();
        }
    }
}