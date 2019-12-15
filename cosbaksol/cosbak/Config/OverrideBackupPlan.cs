using System;

namespace Cosbak.Config
{
    public class OverrideBackupPlan
    {
        public int? RetentionInDays { get; set; }

        public TimeSpan? Rpo { get; set; }

        public OverrideBackupOptions Included { get; set; } = new OverrideBackupOptions();

        public void Validate()
        {
            if (RetentionInDays <= 0)
            {
                throw new CosbakException($"{nameof(RetentionInDays)} must be positive");
            }
            if (Rpo <= TimeSpan.Zero)
            {
                throw new CosbakException($"{nameof(Rpo)} must be positive");
            }
            Included.Validate();
        }
    }
}