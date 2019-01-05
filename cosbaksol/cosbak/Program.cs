using AppInsights.TelemetryInitializers;
using Cosbak.CommandContext;
using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Cosbak
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            Console.WriteLine($"cosbak - Cosmos DB Backup Solution - Version {AppVersion.FullVersion}");
            Console.WriteLine();

            if (args.Length == 0)
            {
                DisplayBasicHelp();
            }
            else
            {
                try
                {
                    await BranchCommandAsync(args[0], args.Skip(1));
                }
                catch (BackupException ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        #region Help
        private static void DisplayBasicHelp()
        {
            Console.WriteLine("Here are the base commands:");
            Console.WriteLine();
            Console.WriteLine("backup:\t\t\t\tTake a backup of a collection (or database or account)");
        }

        private static void DisplayBackupHelp()
        {
            Console.WriteLine("usage:");
            Console.WriteLine("\tcosbak backup -f BACKUP_DESCRIPTION_FILE");
            Console.WriteLine("\tcosbak backup -ca COSMOS_ACCOUNT_NAME -ck COSMOS_ACCOUNT_KEY [-cf COSMOS_FILTER] "
                + "-sa STORAGE_ACCOUNT_NAME -sc STORAGE_CONTAINER [-sp STORAGE_PREFIX] [-st STORAGE_TOKEN] [-sk STORAGE_KEY] "
                + "[-ak APPLICATION_INSIGHTS_INSTRUMENTATION_KEY -ar APPLICATION_INSIGHTS_ROLE]");
        }
        #endregion

        private static async Task BranchCommandAsync(string command, IEnumerable<string> args)
        {
            switch (command)
            {
                case "backup":
                    await BackupAsync(args);

                    return;

                default:
                    Console.WriteLine($"Command '{command}' unknown");
                    DisplayBasicHelp();

                    return;
            }
        }

        private static async Task BackupAsync(IEnumerable<string> args)
        {
            var switches = ImmutableSortedDictionary<string, Action<BackupContext, string>>
                .Empty
                .Add("f", (c, a) => c.File = a)
                .Add("ca", (c, a) => c.CosmosAccountName = a)
                .Add("ck", (c, a) => c.CosmosAccountKey = a)
                .Add("cf", (c, a) => c.CosmosFilter = a)
                .Add("sa", (c, a) => c.StorageAccountName = a)
                .Add("sc", (c, a) => c.StorageContainer = a)
                .Add("sp", (c, a) => c.StoragePrefix = a)
                .Add("st", (c, a) => c.StorageToken = a)
                .Add("sk", (c, a) => c.StorageKey = a)
                .Add("ak", (c, a) => c.ApplicationInsightsKey = a)
                .Add("ar", (c, a) => c.ApplicationInsightsRole = a);
            var context = ReadContext(args, switches);
            var description = await InferDescriptionAsync(context);

            InitializeAppInsights(description.AppInsights);

            var telemetry = new TelemetryClient();
            var cosmosGateways = from a in description.CosmosAccounts
                                 select new CosmosDbAccountGateway(a.Name, a.Key, a.Filters);
            var controller = new BackupController(
                telemetry,
                cosmosGateways,
                new StorageGateway(
                    description.Storage.Name,
                    description.Storage.Container,
                    description.Storage.Token,
                    description.Storage.Key,
                    description.Storage.Prefix));

            try
            {
                await controller.BackupAsync();
            }
            catch (Exception ex)
            {
                telemetry.TrackException(ex);
            }
            finally
            {
                telemetry.Flush();
            }
        }

        private async static Task<BackupDescription> InferDescriptionAsync(BackupContext context)
        {
            try
            {
                var description = string.IsNullOrWhiteSpace(context.File)
                    ? CreateFromContext(context)
                    : await ReadDescriptionAsync(context.File);

                description.Validate();

                return description;
            }
            catch (BackupException)
            {
                DisplayBackupHelp();
                Console.WriteLine();

                throw;
            }
        }

        private static BackupDescription CreateFromContext(BackupContext context)
        {
            var description = new BackupDescription
            {
                CosmosAccounts = new[]
                {
                    new CosmosAccountDescription
                    {
                        Name=context.CosmosAccountName,
                        Key=context.CosmosAccountKey
                    }
                },
                Storage = new StorageDescription
                {
                    Name = context.StorageAccountName,
                    Container = context.StorageContainer,
                    Prefix = context.StoragePrefix,
                    Token = context.StorageToken,
                    Key = context.StorageKey
                }
            };

            if (!string.IsNullOrWhiteSpace(context.CosmosFilter))
            {
                description.CosmosAccounts[0].Filters = new[]
                {
                    context.CosmosFilter
                };
            }
            if (!string.IsNullOrWhiteSpace(context.ApplicationInsightsKey)
                || !string.IsNullOrWhiteSpace(context.ApplicationInsightsRole))
            {
                description.AppInsights = new AppInsightsDescription
                {
                    Key = context.ApplicationInsightsKey,
                    Role = context.ApplicationInsightsRole
                };
            }

            return description;
        }

        private static CONTEXT ReadContext<CONTEXT>(
            IEnumerable<string> args,
            IImmutableDictionary<string, Action<CONTEXT, string>> switches) where CONTEXT : new()
        {
            var context = new CONTEXT();

            while (args.Any())
            {
                var first = args.First();

                if (string.IsNullOrWhiteSpace(first) || first.Length < 2 || first[0] != '-')
                {
                    throw new BackupException($"'{first}' isn't a switch");
                }

                var switchLabel = first.Substring(1);

                if (!switches.ContainsKey(switchLabel))
                {
                    throw new BackupException($"'{switchLabel}' isn't a valid switch");
                }
                if (!args.Skip(1).Any())
                {
                    throw new BackupException($"'{switchLabel}' doesn't have an argument");
                }

                var argument = args.Skip(1).First();

                switches[switchLabel](context, argument);
                args = args.Skip(2);
            }

            return context;
        }

        private static async Task<BackupDescription> ReadDescriptionAsync(string path)
        {
            var uri = new Uri(path, UriKind.RelativeOrAbsolute);
            var content = uri.IsAbsoluteUri
                ? await GetWebContent(uri)
                : await File.ReadAllTextAsync(uri.ToString());
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(new CamelCaseNamingConvention())
                .Build();
            var description = deserializer.Deserialize<BackupDescription>(content);

            return description;
        }

        private async static Task<string> GetWebContent(Uri filePath)
        {
            var request = WebRequest.Create(filePath);

            using (var response = await request.GetResponseAsync())
            using (var stream = response.GetResponseStream())
            using (var reader = new StreamReader(stream))
            {
                return await reader.ReadToEndAsync();
            }
        }

        private static void InitializeAppInsights(AppInsightsDescription appInsights)
        {
            if (appInsights != null)
            {
                TelemetryConfiguration.Active.InstrumentationKey = appInsights.Key;

                if (!string.IsNullOrWhiteSpace(appInsights.Role))
                {
                    TelemetryConfiguration.Active.TelemetryInitializers.Add(new RoleNameInitializer(appInsights.Role));
                }
            }
        }
    }
}