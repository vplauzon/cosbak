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

            if (args.Length == 0)
            {
                DisplayBasicHelp();
            }
            else
            {
                BranchCommand(args[0], args.Skip(1));
            }
        }

        private static void BranchCommand(string command, IEnumerable<string> args)
        {
            switch (command)
            {
                case "backup":
                    Console.WriteLine("Backup...  work in progress");

                    return;

                default:
                    Console.WriteLine($"Command '{command}' unknown");
                    DisplayBasicHelp();

                    return;
            }
        }

        private static void DisplayBasicHelp()
        {
            Console.WriteLine("Here are the base commands:");
            Console.WriteLine();
            Console.WriteLine("backup:\t\t\t\tTake a backup of a collection (or database or account)");
        }
    }
}