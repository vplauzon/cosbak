using System;
using System.Collections.Generic;

namespace Cosbak.Commands
{
    public class BackupCommandParameters
    {
        public string? ConfigPath { get; set; }

        public BackupMode Mode { get; set; } = BackupMode.Continuous;
    }
}