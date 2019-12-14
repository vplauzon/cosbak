using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Cosbak.Controllers.Backup
{
    public abstract class CommandBase<CONFIG>
    {
        public CONFIG ReadParameters(IEnumerable<string> args)
        {
            var config = NewConfig();
            var switchToAction = GetSwitchToAction();
            var switchValues = ReadValues(switchToAction, args);

            foreach (var a in switchValues)
            {
                var switchLabel = a.Key;
                var switchValue = a.Value;
                var action = switchToAction[switchLabel];

                action(config, switchValue);
            }

            return config;
        }

        protected abstract CONFIG NewConfig();

        protected abstract IImmutableDictionary<string, Action<CONFIG, string>> GetSwitchToAction();

        private IImmutableDictionary<string, string> ReadValues(
            IImmutableDictionary<string, Action<CONFIG, string>> switchToAction,
            IEnumerable<string> args)
        {
            var switchValues = ImmutableSortedDictionary<string, string>.Empty;

            while (args.Any())
            {
                var first = args.First();

                if (string.IsNullOrWhiteSpace(first) || first.Length < 2 || first[0] != '-')
                {
                    throw new CosbakException($"'{first}' isn't a valid switch");
                }

                var switchLabel = first.Substring(1);

                if (!switchToAction.ContainsKey(switchLabel))
                {
                    throw new CosbakException($"'{switchLabel}' isn't a valid switch label");
                }
                if (!args.Skip(1).Any())
                {
                    throw new CosbakException($"'{switchLabel}' doesn't have an argument");
                }

                var argument = args.Skip(1).First();

                switchValues = switchValues.Add(switchLabel, argument);
                args = args.Skip(2);
            }

            return switchValues;
        }
    }
}