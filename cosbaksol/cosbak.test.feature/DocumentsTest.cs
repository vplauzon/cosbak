using Cosbak.Commands;
using Cosbak.Config;
using Cosbak.Controllers;
using Microsoft.Azure.Cosmos;
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
            var sourceContainer = await CosmosCollectionRental.GetCollectionAsync("empty");
            var targetContainer = await CosmosCollectionRental.GetCollectionAsync("empty-restore");
            
            await BackupRestoreAsync(sourceContainer, targetContainer);
            await CollectionComparer.CompareDocumentCountAsync(sourceContainer, targetContainer);
        }

        private static async Task BackupRestoreAsync(Container sourceContainer, Container targetContainer)
        {
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
                        Collection= sourceContainer.Id
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
                    Collection = targetContainer.Id
                }
            };

            await metaController.BackupAsync(backupConfiguration, BackupMode.Iterative);
            await metaController.RestoreAsync(restoreConfiguration);
        }
    }
}