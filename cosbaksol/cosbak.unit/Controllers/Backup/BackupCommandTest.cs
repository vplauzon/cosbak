using Cosbak.Controllers.Backup;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace cosbak.unit.Command
{
    public class BackupCommandTest
    {
        #region In Memory
        [Fact]
        public async Task EmptyAsync()
        {
            var command = new BackupCommand();
            var config = await command.ReadParametersAsync(new string[0]);

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
        }

        [Fact]
        public async Task CosmosNameAsync()
        {
            const string NAME = "MyCosmos";
            var command = new BackupCommand();
            var config = await command.ReadParametersAsync(new[] { "-cn", NAME });

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
            Assert.Equal(NAME, config.CosmosAccount.Name);
        }

        [Fact]
        public async Task CosmosNameAndKeyAsync()
        {
            const string NAME = "MyCosmos";
            const string KEY = "123456";
            var command = new BackupCommand();
            var config = await command.ReadParametersAsync(new[] { "-cn", NAME, "-ck", KEY });

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
            Assert.Equal(NAME, config.CosmosAccount.Name);
            Assert.Equal(KEY, config.CosmosAccount.Key);
        }

        [Fact]
        public async Task CosmosKeyAndStorageContainerAsync()
        {
            const string KEY = "123456";
            const string CONTAINER = "acontainer";
            var command = new BackupCommand();
            var config = await command.ReadParametersAsync(new[] { "-ck", KEY, "-sc", CONTAINER });

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
            Assert.Equal(KEY, config.CosmosAccount.Key);
            Assert.Equal(CONTAINER, config.StorageAccount.Container);
        }
        #endregion

        #region With Files
        [Fact]
        public async Task NoSecretAsync()
        {
            var command = new BackupCommand();
            var config = await command.ReadParametersAsync(new string[]
            {
                "-f",
                "Controllers/Backup/no-secrets.yaml",
                "-ck",
                "key",
                "-st",
                "?token"
            });

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
            Assert.Equal("cosbak-sample", config.CosmosAccount.Name);
            Assert.Equal("key", config.CosmosAccount.Key);
            Assert.Equal("X", config.StorageAccount.Name);
            Assert.Equal("Y", config.StorageAccount.Container);
            Assert.Equal("Z", config.StorageAccount.Folder);
            Assert.Equal("?token", config.StorageAccount.Token);
        }

        [Fact]
        public async Task NoSecretOverrideAsync()
        {
            var command = new BackupCommand();
            var config = await command.ReadParametersAsync(new string[]
            {
                "-f",
                "Controllers/Backup/no-secrets.yaml",
                "-ck",
                "key",
                "-cn",
                "mycosmos",
                "-st",
                "?token"
            });

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
            Assert.Equal("mycosmos", config.CosmosAccount.Name);
            Assert.Equal("key", config.CosmosAccount.Key);
            Assert.Equal("X", config.StorageAccount.Name);
            Assert.Equal("Y", config.StorageAccount.Container);
            Assert.Equal("Z", config.StorageAccount.Folder);
            Assert.Equal("?token", config.StorageAccount.Token);
        }

        [Fact]
        public async Task WithFiltersAsync()
        {
            var command = new BackupCommand();
            var config = await command.ReadParametersAsync(new string[]
            {
                "-f",
                "Controllers/Backup/with-filters.yaml"
            });

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
            Assert.True(1 == config.Filters.Length);
            Assert.Equal("mydb.mycoll", config.Filters[0]);
        }
        #endregion
    }
}