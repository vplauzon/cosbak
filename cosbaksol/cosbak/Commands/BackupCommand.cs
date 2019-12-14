using Cosbak.Controllers.Backup;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Cosbak.Commands
{
    public class BackupCommand : CommandBase<BackupCommandParameters>
    {
        protected override BackupCommandParameters NewConfig()
        {
            return new BackupCommandParameters();
        }

        protected override IImmutableDictionary<string, Action<BackupCommandParameters, string>> GetSwitchToAction()
        {
            var switchToAction = ImmutableSortedDictionary<string, Action<BackupCommandParameters, string>>
                .Empty
                .Add("c", (c, value) => c.ConfigPath = value)
                .Add("m", (c, value) => c.Mode = GetMode(value));

            return switchToAction;
        }

        private BackupMode GetMode(string value)
        {
            switch(value.ToLower())
            {
                case "continuous":
                    return BackupMode.Continuous;
                case "iterative":
                    return BackupMode.Iterative;

                default:
                    throw new CosbakException($"'{value}' isn't a valid backup mode");
            }
        }
    }
}