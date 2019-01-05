using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Linq;
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
            var client = new DocumentClient(
                new Uri($"https://{account}.documents.azure.com:443/"),
                key);
            var collectionUri = UriFactory.CreateDocumentCollectionUri(db, collection);
            var random = new Random();
            var tasks = new List<Task>();

            for (int p = 0; p != partitionCount; ++p)
            {
                var partition = CreateRandomPartition(random);

                for (int d = 0; d != docCount / partitionCount; ++d)
                {
                    var content = new
                    {
                        name = CreateRandomString(random, 50),
                        age = random.Next(10, 100),
                        address = new
                        {
                            number = random.Next(1, 40000),
                            street = CreateRandomString(random, 45)
                        },
                        salary = random.NextDouble() * 100000 + 10000,
                        background = CreateRandomString(random, 100),
                        province = CreateRandomString(random, 15)
                    };
                    var document = new Dictionary<string, object>
                    {
                        { "id", CreateRandomString(random, 40) },
                        { partitionKey, partition },
                        {"content", content }
                    };

                    tasks.Add(client.CreateDocumentAsync(collectionUri, document));
                }
                Console.WriteLine($"Partition:  {partition}");
                await Task.WhenAll(tasks.ToArray());
                tasks.Clear();
            }
        }

        private static object CreateRandomPartition(Random random)
        {
            switch (random.Next(0, 3))
            {
                case 0:
                    return random.Next(0, 2) == 0 ? true : false;
                case 1:
                    return random.Next();
                default:
                    return CreateRandomString(random, 30);
            }
        }

        private static string CreateRandomString(Random random, int length)
        {
            var array = from i in Enumerable.Range(0, length)
                        select (char)random.Next((int)'A', (int)'Z' + 1);
            var value = new string(array.ToArray());

            return value;
        }
    }
}