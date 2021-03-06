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
            var targetContainer = await CosmosCollectionRental.GetCollectionAsync(sourceContainer.Id + "-restore");

            await BackupAsync(sourceContainer);
            await RestoreAsync(sourceContainer, targetContainer);
            await CollectionComparer.CompareDocumentCountAsync(sourceContainer, targetContainer);
        }

        [Fact]
        public async Task OneDocument()
        {
            var sourceContainer = await CosmosCollectionRental.GetCollectionAsync("one-document");
            var targetContainer = await CosmosCollectionRental.GetCollectionAsync(sourceContainer.Id + "-restore");

            await sourceContainer.CreateItemAsync(new
            {
                id = "test",
                name = "John",
                age = 40,
                address = new
                {
                    street = "Baker",
                    number = "221B"
                },
                skills = new[] { "jumping", "walking" }
            });
            await BackupAsync(sourceContainer);
            await RestoreAsync(sourceContainer, targetContainer);
            await CollectionComparer.CompareDocumentsAsync(sourceContainer, targetContainer);
        }

        [Fact]
        public async Task MultipleDocsWithOneBackupDocument()
        {
            var sourceContainer = await CosmosCollectionRental.GetCollectionAsync("MultipleDocsWithOneBackupDocument");
            var targetContainer = await CosmosCollectionRental.GetCollectionAsync(sourceContainer.Id + "-restore");

            await sourceContainer.CreateItemAsync(new
            {
                id = "test",
                name = "John",
                age = 40,
                address = new
                {
                    street = "Baker",
                    number = "221B"
                }
            });
            await sourceContainer.CreateItemAsync(new
            {
                id = "test2",
                name = "Jim",
                age = 40.5
            });
            await sourceContainer.CreateItemAsync(new
            {
                id = "test3",
                name = "Bill",
                age = 40,
                isMarried = true
            });
            await BackupAsync(sourceContainer);
            await RestoreAsync(sourceContainer, targetContainer);
            await CollectionComparer.CompareDocumentsAsync(sourceContainer, targetContainer);
        }

        [Fact]
        public async Task MultipleDocsWithMultipleBackupsDocument()
        {
            var sourceContainer = await CosmosCollectionRental.GetCollectionAsync("MultipleDocsWithMultipleBackupsDocument");
            var targetContainer = await CosmosCollectionRental.GetCollectionAsync(sourceContainer.Id + "-restore");

            await sourceContainer.CreateItemAsync(new
            {
                id = "test",
                name = "John",
                age = 40,
                address = new
                {
                    street = "Baker",
                    number = "221B"
                }
            });
            await BackupAsync(sourceContainer);
            await sourceContainer.CreateItemAsync(new
            {
                id = "test2",
                name = "Jim",
                age = 40.5
            });
            await BackupAsync(sourceContainer);
            await sourceContainer.CreateItemAsync(new
            {
                id = "test3",
                name = "Bill",
                age = 40,
                isMarried = true
            });
            await BackupAsync(sourceContainer);
            await RestoreAsync(sourceContainer, targetContainer);
            await CollectionComparer.CompareDocumentsAsync(sourceContainer, targetContainer);
        }

        private static async Task BackupAsync(Container sourceContainer)
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
                        Db = CosmosCollectionRental.DatabaseName,
                        Collection = sourceContainer.Id
                    }
                }
            };

            await metaController.BackupAsync(backupConfiguration, BackupMode.Iterative);
        }

        private static async Task RestoreAsync(Container sourceContainer, Container targetContainer)
        {
            var metaController = new MetaController();
            var restoreConfiguration = new RestoreConfiguration
            {
                CosmosAccount = CosmosCollectionRental.CosmosConfiguration,
                StorageAccount = Storage.StorageConfiguration,
                SourceCollection = new CompleteCollectionConfiguration
                {
                    Account = CosmosCollectionRental.CosmosConfiguration.Name,
                    Db = CosmosCollectionRental.DatabaseName,
                    Collection = sourceContainer.Id,
                },
                TargetCollection = new CollectionConfiguration
                {
                    Db = CosmosCollectionRental.DatabaseName,
                    Collection = targetContainer.Id
                }
            };

            await metaController.RestoreAsync(restoreConfiguration);
        }
    }
}