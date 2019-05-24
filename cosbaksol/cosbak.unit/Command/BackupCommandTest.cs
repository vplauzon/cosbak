using Cosbak.Command;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace cosbak.unit.Command
{
    public class BackupCommandTest
    {
        [Fact]
        public async Task EmptyAsync()
        {
            var command = new BackupCommand();
            var config = await command.ExtractDescriptionAsync(new string[0]);

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
        }

        [Fact]
        public async Task CosmosNameAsync()
        {
            const string NAME = "MyCosmos";
            var command = new BackupCommand();
            var config = await command.ExtractDescriptionAsync(new[] { "-cn", NAME });

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
            var config = await command.ExtractDescriptionAsync(new[] { "-cn", NAME, "-ck", KEY });

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
            var config = await command.ExtractDescriptionAsync(new[] { "-ck", KEY, "-sc", CONTAINER });

            Assert.NotNull(config);
            Assert.NotNull(config.CosmosAccount);
            Assert.NotNull(config.StorageAccount);
            Assert.Equal(KEY, config.CosmosAccount.Key);
            Assert.Equal(CONTAINER, config.StorageAccount.Container);
        }
    }
}