using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Cosbak.Command
{
    public abstract class CommandBase<CONFIG> where CONFIG : new()
    {
        private readonly IImmutableDictionary<string, Action<CONFIG, string>> _switchToAction;
        private readonly Action<CONFIG> _createSubSections;

        protected CommandBase(
            Action<CONFIG> createSubSections,
            IImmutableDictionary<string, Action<CONFIG, string>> switchToAction)
        {
            _createSubSections = createSubSections;
            _switchToAction = switchToAction
                ?? throw new ArgumentNullException(nameof(switchToAction));
        }

        public async Task<CONFIG> ExtractDescriptionAsync(IEnumerable<string> args)
        {
            var switchValues = ReadValues(args);
            var config = switchValues.ContainsKey("f")
                ? await ReadDescriptionAsync(switchValues["f"])
                : new CONFIG();

            _createSubSections(config);
            foreach (var a in switchValues)
            {
                var switchLabel = a.Key;
                var switchValue = a.Value;

                _switchToAction[switchLabel](config, switchValue);
            }

            return config;
        }

        private async Task<CONFIG> ReadDescriptionAsync(string filePath)
        {
            if (!File.Exists(filePath))
            {
                throw new CosbakException($"There is no configuration file at '{filePath}'");
            }

            var content = await File.ReadAllTextAsync(filePath);
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(new CamelCaseNamingConvention())
                .Build();
            var description = deserializer.Deserialize<CONFIG>(content);

            return description;
        }

        private ImmutableSortedDictionary<string, string> ReadValues(IEnumerable<string> args)
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

                if (!_switchToAction.ContainsKey(switchLabel))
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