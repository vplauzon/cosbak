using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            Console.WriteLine("usage:  cosbak backup BACKUP_DESCRIPTION_FILE");
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
            if (!args.Any())
            {
                Console.WriteLine("cosbak backup error:  backup description file is required");
                DisplayBackupHelp();
            }
            else
            {
                var filePath = args.First();
                var content = await File.ReadAllTextAsync(filePath);
                var deserializer = new DeserializerBuilder()
                    .WithNamingConvention(new CamelCaseNamingConvention())
                    .Build();
                var description = deserializer.Deserialize<BackupDescription>(content);

                try
                {
                    description.Validate();
                }
                catch (BackupException ex)
                {
                    Console.WriteLine($"Backup Description validation error:  {ex.Message}");

                    return;
                }

                var cosmosGateways = from a in description.Accounts
                                     select new CosmosDbAccountGateway(a.Name, a.Key, a.Filters);
                var controller = new BackupController(
                    cosmosGateways,
                    new StorageGateway(description.Storage),
                    description.Ram != null ? description.Ram.Backup : null);

                await controller.BackupAsync();
            }
        }
    }
}