using System;

namespace Cosbak.Config
{
    public class BackupPlan
    {
        public int RetentionInDays { get; set; } = 30;

        public TimeSpan Rpo { get; set; } = TimeSpan.FromMinutes(15);

        public BackupOptions Included { get; set; } = new BackupOptions();

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