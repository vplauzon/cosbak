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
            var backupConfiguration = new BackupConfiguration
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
            var restoreConfiguration = new RestoreConfiguration
            {
                CosmosAccount = backupConfiguration.CosmosAccount,
                StorageAccount = backupConfiguration.StorageAccount,
                SourceCollection = new CompleteCollectionConfiguration
                {
                    Account = backupConfiguration.CosmosAccount.Name,
                    Db = CosmosCollectionRental.DatabaseName,
                    Collection = backupConfiguration.Collections[0].Collection,
                },
                TargetCollection = new CollectionConfiguration
                {
                    Db = CosmosCollectionRental.DatabaseName,
                    Collection = $"{backupConfiguration.Collections[0].Collection}-restore"
                }
            };

            await metaController.BackupAsync(backupConfiguration, BackupMode.Iterative);
            await metaController.RestoreAsync(restoreConfiguration);
        }
    }
}