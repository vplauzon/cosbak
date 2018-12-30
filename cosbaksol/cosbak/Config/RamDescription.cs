using System;

namespace cosbak.Config
{
    public class RamDescription
    {
        private const int MIN_BACKUP_RAM = 20;

        public int? Backup { get; set; }

        internal void Validate()
        {
            if (Backup != null && (Backup < MIN_BACKUP_RAM))
            {
                throw new BackupException("RAM Description's backup must be at least 20Mb");
            }
        }
    }
}