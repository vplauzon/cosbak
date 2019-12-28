using Cosbak.Config;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace cosbak.test.feature
{
    public static class CosmosCollectionRental
    {
        private static Database? _db;
        private static int _collectionCount = new Random().Next(0, 1000);

        public static CosmosAccountConfiguration CosmosConfiguration { get; } =
            CreateCosmosConfiguration();

        public static string DatabaseName { get; } = "test";

        public static async Task<Container> GetCollectionAsync(
            string name,
            string partitionPath = "/part")
        {
            if (_db == null)
            {
                _db = await CreateDatabaseAsync();
            }

            var collectionCount = Interlocked.Increment(ref _collectionCount);

            var collectionName = $"{name}-{collectionCount}";
            var response = await _db.CreateContainerAsync(collectionName, partitionPath);

            return response.Container;
        }

        public static async Task DeleteDatabaseAsync()
        {
            if (_db != null)
            {
                await _db.DeleteAsync();
            }
        }

        private static async Task<Database> CreateDatabaseAsync()
        {
            var client = CreateClient();
            var response = await client.CreateDatabaseIfNotExistsAsync(DatabaseName, 400);

            return response.Database;
        }

        private static CosmosClient CreateClient()
        {
            return new CosmosClient(
                $"https://{CosmosConfiguration.Name}.documents.azure.com:443/",
                CosmosConfiguration.Key);
        }

        private static CosmosAccountConfiguration CreateCosmosConfiguration()
        {
            try
            {
                var account = Environment.GetEnvironmentVariable("cosmos.account");
                var key = Environment.GetEnvironmentVariable("cosmos.key");

                if (string.IsNullOrWhiteSpace(account) || string.IsNullOrWhiteSpace(key))
                {
                    var json = File.ReadAllText("Properties\\launchSettings.json");
                    var root = JsonSerializer.Deserialize<JsonElement>(json);
                    var variables = root
                        .GetProperty("profiles")
                        .GetProperty("cosbak.test.feature")
                        .GetProperty("environmentVariables");

                    account = variables.GetProperty("cosmos.account").GetString();
                    key = variables.GetProperty("cosmos.key").GetString();
                }

                return new CosmosAccountConfiguration
                {
                    Name = account,
                    Key = key
                };
            }
            catch (Exception ex)
            {
                throw new ArgumentNullException("Can't find account & key in environment variable or launchSettings.json", ex);
            }
        }
    }
}