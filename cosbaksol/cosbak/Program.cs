using System;
using System.Collections.Generic;
using System.Linq;

namespace cosbak
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine($"cosbak - Cosmos DB Backup Solution - Version {AppVersion.FullVersion}");
            Console.WriteLine();

            if (args.Length == 0)
            {
                DisplayBasicHelp();
            }
            else
            {
                BranchCommand(args[0], args.Skip(1));
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

        private static void BranchCommand(string command, IEnumerable<string> args)
        {
            switch (command)
            {
                case "backup":
                    Backup(args);

                    return;

                default:
                    Console.WriteLine($"Command '{command}' unknown");
                    DisplayBasicHelp();

                    return;
            }
        }

        private static void Backup(IEnumerable<string> args)
        {
            if (!args.Any())
            {
                Console.WriteLine("cosbak backup error:  backup description file is required");
                DisplayBackupHelp();
            }
            else
            {
            }
        }
    }
}