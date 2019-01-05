using System;
using System.Threading.Tasks;

namespace cosmos_seed
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            Console.WriteLine($"cosmos-seed - Cosmos DB seeding");
            Console.WriteLine();

            if (args.Length != 7)
            {
                DisplayBasicHelp();
            }
            else
            {
                await SeedCollectionAsync(args[0], args[1], args[2], args[3], int.Parse(args[4]), int.Parse(args[5]), args[6]);
            }
        }

        #region Help
        private static void DisplayBasicHelp()
        {
            Console.WriteLine("usage:  cosmos-seed <account name> <database name> <collection name> <partition key> <number of documents> <number of partitions> <key>");
        }
        #endregion

        private async static Task SeedCollectionAsync(
            string account,
            string db,
            string collection,
            string partitionKey,
            int docCount,
            int partitionCount,
            string key)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}