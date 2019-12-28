using Cosbak.Commands;
using Cosbak.Config;
using Cosbak.Controllers;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Xunit;

namespace cosbak.test.feature
{
    [Collection("Cosmos collection")]
    public class DocumentsTest
    {
        [Fact]
        public async Task EmptyBackup()
        {
            var container = await CosmosCollectionRental.GetCollectionAsync("empty");
            var metaController = new MetaController();
            var configuration = new BackupConfiguration
            {
                CosmosAccount = CosmosCollectionRental.CosmosConfiguration,
                StorageAccount = Storage.StorageConfiguration,
                Collections = new[]
                {
                    new CollectionBackupPlanOverride
                    {
                        Db=CosmosCollectionRental.DatabaseName,
                        Collection= container.Id
                    }
                }
            };

            await metaController.BackupAsync(configuration, BackupMode.Iterative);
        }
    }
}