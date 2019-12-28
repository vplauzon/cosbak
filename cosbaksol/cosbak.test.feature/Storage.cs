using Cosbak.Config;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;

namespace cosbak.test.feature
{
    public static class Storage
    {
        public static StorageAccountConfiguration StorageConfiguration { get; } =
            CreateStorageConfiguration();

        private static StorageAccountConfiguration CreateStorageConfiguration()
        {
            try
            {
                var account = Environment.GetEnvironmentVariable("storage.account");
                var key = Environment.GetEnvironmentVariable("storage.key");
                var now = DateTime.Now;

                if (string.IsNullOrWhiteSpace(account) || string.IsNullOrWhiteSpace(key))
                {
                    var json = File.ReadAllText("Properties\\launchSettings.json");
                    var root = JsonSerializer.Deserialize<JsonElement>(json);
                    var variables = root
                        .GetProperty("profiles")
                        .GetProperty("cosbak.test.feature")
                        .GetProperty("environmentVariables");

                    account = variables.GetProperty("storage.account").GetString();
                    key = variables.GetProperty("storage.key").GetString();
                }

                return new StorageAccountConfiguration
                {
                    Name = account,
                    Key = key,
                    Container = "test",
                    Folder = $"{now.Year}-{now.Month}-{now.Day}.{now.Hour}.{now.Minute}.{now.Second}"
                };
            }
            catch (Exception ex)
            {
                throw new ArgumentNullException("Can't find account & key in environment variable or launchSettings.json", ex);
            }
        }
    }
}