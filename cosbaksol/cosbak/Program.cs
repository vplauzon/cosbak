using Cosbak.Commands;
using Cosbak.Config;
using Cosbak.Controllers;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

[assembly: InternalsVisibleTo("cosbak.test.feature")]

namespace Cosbak
{
    class Program
    {
        static async Task Main(string[] args)
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
            Console.WriteLine("restore:\t\t\t\tRestore a collection");
        }

        private static void DisplayBackupHelp()
        {
            Console.WriteLine("usage:");
            Console.WriteLine("\tcosbak backup -c COSBAK_CONFIG_PATH [-m continuous/iterative]");
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

                case "restore":
                case "info":
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
            if (args.Any() && args.First() == "-h")
            {
                DisplayBackupHelp();
            }
            else
            {
                var command = new BackupCommand();
                var parameters = command.ReadParameters(args);

                if (parameters.ConfigPath == null)
                {
                    throw new CosbakException("Switch 'c' not specified, i.e. path to configuration");
                }
                else
                {
                    var configuration = await LoadBackupConfigurationAsync(parameters.ConfigPath);
                    var metaController = new MetaController();

                    await metaController.BackupAsync(configuration, parameters.Mode);
                }
            }
        }

        private static async Task<BackupConfiguration> LoadBackupConfigurationAsync(string configPath)
        {
            try
            {
                var content = await File.ReadAllTextAsync(configPath);
                var deserializer = new DeserializerBuilder()
                    .WithNamingConvention(CamelCaseNamingConvention.Instance)
                    .IgnoreUnmatchedProperties()
                    .Build();
                var description = deserializer.Deserialize<BackupConfiguration>(content);

                return description;
            }
            catch (DirectoryNotFoundException)
            {
                throw new CosbakException("Folder for backup configuration not found");
            }
            catch (FileNotFoundException ex)
            {
                throw new CosbakException($"File '{ex.FileName}' not found");
            }
        }
    }
}