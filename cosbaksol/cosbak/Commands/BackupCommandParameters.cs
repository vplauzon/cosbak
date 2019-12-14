using System;
using System.Collections.Generic;

namespace Cosbak.Controllers.Backup
{
    public class BackupCommandParameters
    {
        public string? ConfigPath { get; set; }

        public BackupMode Mode { get; set; } = BackupMode.Continuous;
    }
}