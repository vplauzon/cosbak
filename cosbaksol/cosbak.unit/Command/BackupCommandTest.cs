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
    }
}