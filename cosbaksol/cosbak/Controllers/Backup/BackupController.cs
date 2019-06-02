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
                var tasks = from p in partitions
                            select BackupPartitionContentAsync(
                                p,
                                storageCollection.GetPartition(p.Id),
                                context.Add("partition", p.Id));

                await Task.WhenAll(tasks);
                storageCollection.UpdateContent(destinationTimeStamp.Value);
            }
        }

        private async Task BackupPartitionContentAsync(
            ICosmosPartitionController cosmosPartition,
            IStoragePartitionController storagePartitionController,
            IImmutableDictionary<string, string> context)
        {
            _logger.WriteEvent("Backup-Start-Partition", context);

            var partitionPathParts = cosmosPartition.PartitionPath.Split('/').Skip(1);
            var feed = cosmosPartition.GetChangeFeed();
            var metaStream = new MemoryStream();
            var contentStream = new MemoryStream();
            var pendingStorageTask = Task.CompletedTask;
            var batchId = 0;

            while (feed.HasMoreResults)
            {
                var batch = await feed.GetBatchAsync();
                var batchContext = context.Add("batch", batchId.ToString());

                ++batchId;
                metaStream.SetLength(0);
                contentStream.SetLength(0);
                using (var writer = new BinaryWriter(metaStream, Encoding.UTF8, true))
                {
                    foreach (var doc in batch)
                    {
                        var metaData =
                            DocumentSpliter.Write(doc, partitionPathParts, writer);

                        metaData.Write(writer);
                    }
                    writer.Flush();
                }
                metaStream.Flush();
                contentStream.Flush();
                metaStream.Position = 0;
                contentStream.Position = 0;

                _logger.WriteEvent(
                    "Backup-End-Partition-Batch-Count",
                    batchContext,
                    count: batch.Count);
                _logger.WriteEvent(
                    "Backup-End-Partition-Batch-MetaSize",
                    batchContext,
                    count: metaStream.Length);
                _logger.WriteEvent(
                    "Backup-End-Partition-Batch-ContentSize",
                    batchContext,
                    count: contentStream.Length);
                await pendingStorageTask;
                pendingStorageTask = storagePartitionController.WriteBatchAsync(
                    metaStream,
                    contentStream);
            }
            await pendingStorageTask;
            _logger.WriteEvent("Backup-End-Partition", context);
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
    }
}