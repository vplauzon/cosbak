using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public class BackupController
    {
        private static readonly TimeSpan WAIT_FOR_CURRENT_BACKUP_PERIOD = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan WAIT_FOR_CURRENT_BACKUP_TOTAL = TimeSpan.FromSeconds(15);

        private readonly ILogger _logger;
        private readonly IBackupStorageController _storageController;
        private readonly IBackupCosmosController _cosmosController;

        public BackupController(
            ILogger logger,
            IBackupCosmosController cosmosController,
            IBackupStorageController storageController)
        {
            _logger = logger;
            _storageController = storageController;
            _cosmosController = cosmosController;
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
                    var recordCount = await BackupCollectionContentAsync(
                        cosmosCollection, storageCollection, context);

                    _logger.Display($"{recordCount} records backed up", context);
                    _logger.WriteEvent("Backup-End-Records", context, count: recordCount);
                    _logger.WriteEvent("Backup-End-Collection", context);
                }
                finally
                {
                    await storageCollection.ReleaseAsync();
                }
            }
            _logger.WriteEvent("Backup-End");
        }

        private async Task<long> BackupCollectionContentAsync(
            ICosmosCollectionController cosmosCollection,
            IStorageCollectionController storageCollection,
            IImmutableDictionary<string, string> context)
        {
            var destinationTimeStamp =
                await GetDestinationTimeStampAsync(cosmosCollection, storageCollection);

            if (destinationTimeStamp == null)
            {
                _logger.WriteEvent("Backup-Collection-NoNewContent", context);

                return 0;
            }
            else
            {
                var partitions = await cosmosCollection.GetPartitionsAsync();

                _logger.Display($"{partitions.Length} partitions");

                var tasks = from p in partitions
                            select BackupPartitionContentAsync(
                                p,
                                storageCollection.GetPartition(p.Id),
                                storageCollection.LastContentTimeStamp,
                                context.Add("partition", p.Id));
                var recordCounts = await Task.WhenAll(tasks);

                storageCollection.UpdateContent(destinationTimeStamp.Value);

                return recordCounts.Sum();
            }
        }

        private async Task<long> BackupPartitionContentAsync(
            ICosmosPartitionController cosmosPartition,
            IStoragePartitionController storagePartitionController,
            long? lastContentTimeStamp,
            IImmutableDictionary<string, string> context)
        {
            _logger.WriteEvent("Backup-Start-Partition", context);

            var partitionPathParts = cosmosPartition.PartitionPath.Split('/').Skip(1);
            var feed = cosmosPartition.GetChangeFeed(lastContentTimeStamp);
            var metaStream = new MemoryStream();
            var contentStream = new MemoryStream();
            var pendingStorageTask = Task.CompletedTask;
            var batchId = 0;
            var recordCount = (long)0;

            while (feed.HasMoreResults)
            {
                var batch = await feed.GetBatchAsync();
                var batchContext = context.Add("batch", batchId.ToString());

                ++batchId;
                await pendingStorageTask;
                metaStream.SetLength(0);
                contentStream.SetLength(0);
                using (var metaWriter = new BinaryWriter(metaStream, Encoding.UTF8, true))
                using (var contentWriter = new BinaryWriter(contentStream, Encoding.UTF8, true))
                {
                    foreach (var doc in batch)
                    {
                        var metaData =
                            DocumentSpliter.WriteContent(doc, partitionPathParts, contentWriter);

                        metaData.Write(metaWriter);
                        ++recordCount;
                    }
                    metaWriter.Flush();
                    contentWriter.Flush();
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
                pendingStorageTask = storagePartitionController.WriteBatchAsync(
                    metaStream,
                    contentStream);
            }
            await pendingStorageTask;
            _logger.WriteEvent("Backup-End-Partition", context);

            return recordCount;
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