using AppInsights.TelemetryInitializers;
using Cosbak.Command;
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
                catch (CosbakException ex)
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

                case "index":
                case "restore":
                case "rotate":
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
            var command = new BackupCommand();
            var description = await command.ExtractDescriptionAsync(args);
            //var storageGateway = CreateStorageGateway(context.FolderPath);
            //var description = await InferDescriptionAsync(storageGateway, context);
            //var telemetry = new TelemetryClient();

            //InitializeAppInsights(description.AppInsights);

            //try
            //{
            //    var cosmosGateway = new CosmosDbAccountGateway(description.CosmosAccount.Name, description.CosmosAccount.Key, description.Plan.Filters);
            //    var controller = new BackupController(
            //        telemetry,
            //        cosmosGateway,
            //        storageGateway);

            //    await controller.BackupAsync();
            //}
            //catch (Exception ex)
            //{
            //    telemetry.TrackException(ex);
            //}
            //finally
            //{
            //    telemetry.Flush();
            //}
        }

        private static IStorageGateway CreateStorageGateway(string folderUri)
        {
            if (string.IsNullOrWhiteSpace(folderUri))
            {
                throw new CosbakException("Folder Uri (-f) is required for backups");
            }

            var uri = new Uri(folderUri, UriKind.RelativeOrAbsolute);

            if (!uri.IsAbsoluteUri)
            {
                throw new CosbakException("Folder must be an Azure Storage container or container's folder");
            }

            return StorageGateway.Create(uri);
        }
    }
}