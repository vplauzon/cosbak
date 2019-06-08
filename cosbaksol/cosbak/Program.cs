using Cosbak.Config;
using Cosbak.Controllers.Backup;
using Cosbak.Controllers.Index;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
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
            Console.WriteLine("\tcosbak backup -f COSBAK_CONFIG_PATH");
            Console.WriteLine("\tcosbak backup "
                + "-cn COSMOS_ACCOUNT_NAME "
                + "-ck COSMOS_ACCOUNT_KEY "
                + "-sn STORAGE_ACCOUNT_NAME "
                + "[-sc STORAGE_ACCOUNT_CONTAINER] "
                + "[-sf STORAGE_ACCOUNT_FOLDER] "
                + "[-sk STORAGE_ACCOUNT_KEY] "
                + "[-st STORAGE_ACCOUNT_TOKEN]");
            Console.WriteLine();
        }

        private static void DisplayIndexHelp()
        {
            Console.WriteLine("usage:");
            Console.WriteLine("\tcosbak index -f COSBAK_CONFIG_PATH");
            Console.WriteLine("\tcosbak index "
                + "-sn STORAGE_ACCOUNT_NAME "
                + "[-sc STORAGE_ACCOUNT_CONTAINER] "
                + "[-sf STORAGE_ACCOUNT_FOLDER] "
                + "[-sk STORAGE_ACCOUNT_KEY] "
                + "[-st STORAGE_ACCOUNT_TOKEN]");
            Console.WriteLine();
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
                    await IndexAsync(args);
                    return;

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
                ILogger logger = new Logger(storageFacade.ChangeFolder("logs"));

                try
                {
                    var cosmosFacade = new DatabaseAccountFacade(
                        description.CosmosAccount.Name,
                        description.CosmosAccount.Key);
                    var storageController = new BackupStorageController(storageFacade, logger);
                    var cosmosController = new BackupCosmosController(
                        cosmosFacade,
                        logger,
                        description.Filters);
                    var controller = new BackupController(
                        logger,
                        cosmosController,
                        storageController);

                    await controller.BackupAsync();
                }
                catch (Exception ex)
                {
                    logger.DisplayError(ex);
                }
                finally
                {
                    await logger.FlushAsync();
                }
            }
        }

        private static async Task IndexAsync(IEnumerable<string> args)
        {
            var command = new IndexCommand();

            if (args.Any() && args.First() == "-h")
            {
                DisplayIndexHelp();
            }
            else
            {
                var description = await command.ReadDescriptionAsync(args);

                description.Validate();

                var storageFacade = CreateStorageFacade(description.StorageAccount);
                ILogger logger = new Logger(storageFacade.ChangeFolder("logs"));

                try
                {
                    var storageController = new BackupStorageController(storageFacade, logger);
                    var controller = new IndexController(
                        logger);

                    await controller.IndexAsync();
                }
                catch (Exception ex)
                {
                    logger.DisplayError(ex);
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