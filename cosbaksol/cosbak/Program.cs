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
            Console.WriteLine($"cosbak - Cosmos DB Backup - Version {AppVersion.FullVersion}");
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
            Console.WriteLine("backup:\t\t\t\tTake a backup of one or many collections");
            Console.WriteLine("index:\t\t\t\tIndex backup of one or many collections");
            Console.WriteLine("restore:\t\t\t\tRestore a collection");
            Console.WriteLine("rotate:\t\t\t\tRotate backups");
        }

        private static void DisplayBackupHelp()
        {
            Console.WriteLine("usage:");
            Console.WriteLine("\tcosbak backup -f BACKUP_DESCRIPTION_FOLDER "
                + "[-ck COSMOS_ACCOUNT_KEY] "
                + "[-ak APPLICATION_INSIGHTS_INSTRUMENTATION_KEY]");
            Console.WriteLine();
            Console.WriteLine("BACKUP_DESCRIPTION_FOLDER must have a SAS token allowing "
                + "read/write/list/delete on the blob container");
        }
        #endregion

        private static async Task BranchCommandAsync(string command, IEnumerable<string> args)
        {
            switch (command)
            {
                case "backup":
                    await BackupAsync(args);
                    return;

                case "restore":
                case "rotation":
                    Console.Error.WriteLine($"Command '{command}' not supported yet");
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
                .Add("f", (c, a) => c.FolderUri = a)
                .Add("ck", (c, a) => c.CosmosAccountKey = a)
                .Add("ak", (c, a) => c.ApplicationInsightsKey = a);
            var context = ReadContext(args, switches);
            var storageGateway = CreateStorageGateway(context.FolderUri);
            var description = await InferDescriptionAsync(storageGateway, context);
            var telemetry = new TelemetryClient();

            InitializeAppInsights(description.AppInsights);

            try
            {
                var cosmosGateway = new CosmosDbAccountGateway(description.CosmosAccount.Name, description.CosmosAccount.Key, description.Plan.Filters);
                var controller = new BackupController(
                    telemetry,
                    cosmosGateway,
                    storageGateway);

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

        private static IStorageGateway CreateStorageGateway(string folderUri)
        {
            if (string.IsNullOrWhiteSpace(folderUri))
            {
                throw new BackupException("Folder Uri (-f) is required for backups");
            }

            var uri = new Uri(folderUri, UriKind.RelativeOrAbsolute);

            if (!uri.IsAbsoluteUri)
            {
                throw new BackupException("Folder must be an Azure Storage container or container's folder");
            }

            return StorageGateway.Create(uri);
        }

        private async static Task<BackupDescription> InferDescriptionAsync(IStorageGateway storage, BackupContext context)
        {
            try
            {
                var description = await ReadDescriptionAsync(storage);

                if (!string.IsNullOrWhiteSpace(context.CosmosAccountKey)
                    && description.CosmosAccount != null)
                {
                    description.CosmosAccount.Key = context.CosmosAccountKey;
                }
                if (!string.IsNullOrWhiteSpace(context.ApplicationInsightsKey)
                    && description.AppInsights != null)
                {
                    description.AppInsights.Key = context.ApplicationInsightsKey;
                }
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

        private static async Task<BackupDescription> ReadDescriptionAsync(IStorageGateway storage)
        {
            var content = await storage.GetContentAsync("cosmos-backup.yaml");
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(new CamelCaseNamingConvention())
                .Build();
            var description = deserializer.Deserialize<BackupDescription>(content);

            return description;
        }

        private static void InitializeAppInsights(AppInsightsDescription appInsights)
        {
            if (appInsights != null)
            {
                TelemetryConfiguration.Active.InstrumentationKey = appInsights.Key;

                TelemetryConfiguration.Active.TelemetryInitializers.Add(new SessionInitializer());
                if (!string.IsNullOrWhiteSpace(appInsights.Role))
                {
                    TelemetryConfiguration.Active.TelemetryInitializers.Add(new RoleNameInitializer(appInsights.Role));
                }
            }
        }
    }
}