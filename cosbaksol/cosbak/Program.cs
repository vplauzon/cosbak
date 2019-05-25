using AppInsights.TelemetryInitializers;
using Cosbak.Command;
using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Logging;
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
            Console.WriteLine("\tcosbak backup -f BACKUP_CONFIG_PATH");
            Console.WriteLine("\tcosbak backup "
                + "-cn COSMOS_ACCOUNT_NAME "
                + "-ck COSMOS_ACCOUNT_KEY "
                + "-sn STORAGE_ACCOUNT_NAME "
                + "[-sc STORAGE_ACCOUNT_CONTAINER] "
                + "[-sf STORAGE_ACCOUNT_FOLDER] "
                + "[-sk STORAGE_ACCOUNT_KEY] "
                + "[-st STORAGE_ACCOUNT_TOKEN]");
            Console.WriteLine();
            Console.WriteLine("BACKUP_CONFIG_PATH must have a SAS token allowing "
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

            if (args.Any() && args.First() == "-h")
            {
                DisplayBackupHelp();
            }
            else
            {
                var description = await command.ReadDescriptionAsync(args);

                description.Validate();

                var storageFacade = CreateStorageFacade(description.StorageAccount);
                ILogger logger = new Logger(storageFacade);

                try
                {
                    var storageGateway = new StorageBackupGateway(storageFacade, logger);
                    var cosmosGateway = new DatabaseAccountFacade(
                        description.CosmosAccount.Name,
                        description.CosmosAccount.Key,
                        description.Plan.Filters);
                    //var controller = new BackupController(
                    //    telemetry,
                    //    cosmosGateway,
                    //    storageGateway);

                    //await controller.BackupAsync();
                }
                catch (Exception ex)
                {
                    await logger.WriteAsync(new ExceptionTelemetry(ex));
                }
                finally
                {
                    await logger.FlushAsync();
                }
            }
        }

        private static IStorageFacade CreateStorageFacade(StorageAccountDescription description)
        {
            if (string.IsNullOrWhiteSpace(description.Key))
            {
                return StorageFacade.FromToken(
                    description.Name,
                    description.Container,
                    description.Folder,
                    description.Token);
            }
            else
            {
                return StorageFacade.FromKey(
                    description.Name,
                    description.Container,
                    description.Folder,
                    description.Key);
            }
        }
    }
}