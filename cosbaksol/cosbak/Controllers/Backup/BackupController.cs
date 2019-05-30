using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cosbak.Cosmos;
using Cosbak.Storage;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;

namespace Cosbak.Controllers.Backup
{
    public class BackupController
    {
        private static readonly TimeSpan WAIT_FOR_CURRENT_BACKUP_PERIOD = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan WAIT_FOR_CURRENT_BACKUP_TOTAL = TimeSpan.FromSeconds(15);

        private readonly ILogger _logger;
        private readonly IDatabaseAccountFacade _databaseAccount;
        private readonly IStorageFacade _storage;
        private readonly IBackupStorageController _storageController;
        private readonly IBackupCosmosController _cosmosController;

        public BackupController(
            ILogger logger,
            IDatabaseAccountFacade databaseAccount,
            IStorageFacade storage,
            IBackupCosmosController cosmosController,
            IBackupStorageController storageController)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _databaseAccount = databaseAccount ?? throw new ArgumentNullException(nameof(databaseAccount));
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _storageController = storageController ?? throw new ArgumentNullException(nameof(storageController));
            _cosmosController = cosmosController ?? throw new ArgumentNullException(nameof(cosmosController));
        }

        public async Task BackupAsync()
        {
            _logger.Display("Backup...");
            _logger.WriteEvent("Backup-Start");
            foreach (var cosmosCollection in await _cosmosController.GetCollectionsAsync())
            {
                var context = ImmutableDictionary<string, string>
                    .Empty
                    .Add("account", cosmosCollection.Account)
                    .Add("db", cosmosCollection.Database)
                    .Add("collection", cosmosCollection.Collection);

                _logger.WriteEvent("Backup-Start-Collection", context);
                _logger.Display($"Collection {cosmosCollection.Account}"
                    + $".{cosmosCollection.Database}.{cosmosCollection.Collection}");

                var storageCollection = await _storageController.LockMasterAsync(
                    cosmosCollection.Account,
                    cosmosCollection.Database,
                    cosmosCollection.Collection);

                try
                {
                    await BackupCollectionContentAsync(cosmosCollection, storageCollection, context);
                    _logger.WriteEvent("Backup-End-Collection", context);
                }
                finally
                {
                    await storageCollection.ReleaseAsync();
                }
            }
            _logger.WriteEvent("Backup-End");
        }

        private async Task BackupCollectionContentAsync(
            ICosmosCollectionController cosmosCollection,
            IStorageCollectionController storageCollection,
            IImmutableDictionary<string, string> context)
        {
            var destinationTimeStamp =
                await GetDestinationTimeStampAsync(cosmosCollection, storageCollection);

            if (destinationTimeStamp == null)
            {
                _logger.WriteEvent("Backup-Collection-NoNewContent", context);
            }
            else
            {
                var partitions = await cosmosCollection.GetPartitionsAsync();
                var tasks = (from p in partitions
                             select BackupPartitionContentAsync(
                                 p,
                                 storageCollection.GetPartition(p.Id),
                                 context.Add("partition", p.Id))).ToArray();

                storageCollection.UpdateContent(destinationTimeStamp.Value);

                throw new NotImplementedException();
            }
        }

        private async Task BackupPartitionContentAsync(
            ICosmosPartitionController cosmosPartition,
            IStoragePartitionController storagePartitionController,
            IImmutableDictionary<string, string> context)
        {
            _logger.WriteEvent("Backup-Start-Partition", context);
            _logger.WriteEvent("Backup-End-Partition", context);
            await Task.CompletedTask;

            throw new NotImplementedException();
        }

        private async Task<long?> GetDestinationTimeStampAsync(
            ICosmosCollectionController cosmosCollection,
            IStorageCollectionController storageCollection)
        {
            var lastRecordTimeStamp = await cosmosCollection.GetLastRecordTimeStampAsync();

            if (lastRecordTimeStamp == null
                || lastRecordTimeStamp == storageCollection.LastContentTimeStamp)
            {
                return null;
            }
            else
            {
                return lastRecordTimeStamp;
            }
        }

        #region Legacy
        private async Task BackupPartitionAsync(
            string blobPrefix,
            IPartitionFacade partition,
            IImmutableDictionary<string, string> collectionProperties)
        {
            var partitionProperties = collectionProperties.Add("partition", partition.KeyRangeId);

            _logger.WriteEvent("Backup-Start-Partition", partitionProperties);

            var feed = partition.GetChangeFeed();
            var indexPath = blobPrefix + partition.KeyRangeId + ".index";
            var contentPath = blobPrefix + partition.KeyRangeId + ".content";
            var pendingStorageTask = Task.WhenAll(
                _storage.CreateAppendBlobAsync(indexPath),
                _storage.CreateAppendBlobAsync(contentPath));
            long totalIndex = 0, totalContent = 0, totalDocuments = 0;

            while (feed.HasMoreResults)
            {
                var batch = await feed.GetBatchAsync();
                var indexStream = new MemoryStream();
                var contentStream = new MemoryStream();

                using (var writer = new BinaryWriter(indexStream, Encoding.ASCII, true))
                {
                    foreach (var doc in batch)
                    {
                        doc.MetaData.Write(writer);
                        contentStream.Write(doc.Content);
                    }
                }

                indexStream.Flush();
                contentStream.Flush();
                indexStream.Position = 0;
                contentStream.Position = 0;
                totalIndex += indexStream.Length;
                totalContent += contentStream.Length;
                totalDocuments += batch.Length;
                //  Make sure work done on blobs is done
                await pendingStorageTask;
                //  Push more work to storage
                pendingStorageTask = Task.WhenAll(
                    _storage.AppendBlobAsync(indexPath, indexStream),
                    _storage.AppendBlobAsync(contentPath, contentStream));
            }
            //  Make sure storage work is done
            await pendingStorageTask;

            _logger.WriteEvent(
                "Backup-Partition-totalIndex", partitionProperties, count: totalIndex);
            _logger.WriteEvent(
                "Backup-Partition-totalContent", partitionProperties, count: totalContent);
            _logger.WriteEvent(
                "Backup-Partition-totalDocuments", partitionProperties, count: totalDocuments);
            _logger.WriteEvent("Backup-End-Partition", partitionProperties);
        }
        #endregion
    }
}