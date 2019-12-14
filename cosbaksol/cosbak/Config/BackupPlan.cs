using System;

namespace Cosbak.Config
{
    public class BackupPlan
    {
        public int RetentionInDays { get; set; } = 30;

        public int RpoInSeconds { get; set; } = 900;

        public BackupOptions Included { get; set; } = new BackupOptions();

        public void Validate()
        {
            if (RetentionInDays<=0)
            {
                throw new CosbakException("RetentionInDays must be positive");
            }
            if (RpoInSeconds <= 0)
            {
                throw new CosbakException("RpoInSeconds must be positive");
            }
            Included.Validate();
        }
    }
}