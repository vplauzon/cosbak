using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public class BackupScheduler
    {
        #region Inner Types
        private class CollectionPlan
        {
            public CollectionPlan(ICollectionFacade collection, BackupPlan plan)
            {
                Collection = collection;
                Plan = plan;
            }

            public ICollectionFacade Collection { get; }

            public BackupPlan Plan { get; }
        }

        private class Initialized
        {
            public Initialized(IEnumerable<CollectionPlan> collectionPlans)
            {
                CollectionPlans = collectionPlans.ToImmutableArray();
            }

            public IImmutableList<CollectionPlan> CollectionPlans { get; }
        }
        #endregion

        private readonly ILogger _logger;
        private readonly IBackupStorageController _storageController;
        private readonly ICosmosAccountFacade _cosmosFacade;
        private IImmutableList<CollectionBackupPlan> _collectionBackupPlans;
        private Initialized? _initialized;

        public BackupScheduler(
            ILogger logger,
            ICosmosAccountFacade cosmosFacade,
            IStorageFacade storageFacade,
            IImmutableList<CollectionBackupPlan> collectionPlans)
        {
            var storageController = new BackupStorageController(storageFacade, logger);

            _logger = logger;
            _cosmosFacade = cosmosFacade;
            _storageController = storageController;
            _collectionBackupPlans = collectionPlans;
        }

        public async Task InitializeAsync()
        {
            if (_initialized != null || _collectionBackupPlans == null)
            {
                throw new InvalidOperationException("InitializeAsync has already been called");
            }

            var collectionPlans = await GetCollectionPlansAsync(
                _collectionBackupPlans,
                _cosmosFacade).ToEnumerable();

            _initialized = new Initialized(collectionPlans);
        }

        public async Task BackupAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            _logger.Display("Backup...");
            _logger.WriteEvent("Backup-Start");
            foreach (var plan in _initialized.CollectionPlans)
            {
                var logger = _logger
                    .AddContext("account", plan.Collection.Parent.Parent.AccountName)
                    .AddContext("db", plan.Collection.Parent.DatabaseName)
                    .AddContext("collection", plan.Collection.CollectionName);
                var collection = plan.Collection;

                logger.WriteEvent("Backup-Start-Collection");
                logger.Display($"Collection {collection.Parent.Parent.AccountName}"
                    + $".{collection.Parent.DatabaseName}.{collection.CollectionName}");

                var storageCollection = await _storageController.LockLogBlobAsync(
                    collection.Parent.Parent.AccountName,
                    collection.Parent.DatabaseName,
                    collection.CollectionName);

                try
                {
                    var recordCount = await BackupCollectionContentAsync(
                        collection, storageCollection, logger);

                    logger.Display($"{recordCount} records backed up");
                    logger.AddContext("count", recordCount).WriteEvent("Backup-End-Records");
                    logger.WriteEvent("Backup-End-Collection");
                }
                finally
                {
                    await storageCollection.ReleaseAsync();
                }
            }
            _logger.WriteEvent("Backup-End");
        }

        private async static IAsyncEnumerable<CollectionPlan> GetCollectionPlansAsync(
            IImmutableList<CollectionBackupPlan> collectionBackupPlans,
            ICosmosAccountFacade cosmosFacade)
        {
            var byDbs = collectionBackupPlans.GroupBy(p => p.Db).ToDictionary(g => g.Key);
            var dbs = await cosmosFacade.GetDatabasesAsync();

            foreach (var db in dbs)
            {
                if (byDbs.ContainsKey(db.DatabaseName))
                {
                    var byCollections = byDbs[db.DatabaseName].ToDictionary(c => c.Collection);
                    var collections = await db.GetCollectionsAsync();

                    foreach (var coll in collections)
                    {
                        if (byCollections.ContainsKey(coll.CollectionName))
                        {
                            var plan = byCollections[coll.CollectionName].SpecificPlan;

                            yield return new CollectionPlan(coll, plan);
                        }
                    }
                }
            }
        }

        private async Task<long> BackupCollectionContentAsync(
            ICollectionFacade collectionFacade,
            IStorageCollectionController storageCollection,
            ILogger logger)
        {
            var destinationTimeStamp =
                await GetDestinationTimeStampAsync(collectionFacade, storageCollection);

            if (destinationTimeStamp == null)
            {
                logger.WriteEvent("Backup-Collection-NoNewContent");

                return 0;
            }
            else
            {
                var partitions = await collectionFacade.GetPartitionsAsync();

                _logger.Display($"{partitions.Length} partitions");

                var tasks = from p in partitions
                            select BackupPartitionContentAsync(
                                p,
                                storageCollection.GetPartition(p.KeyRangeId),
                                storageCollection.LastContentTimeStamp,
                                logger.AddContext("partition", p.KeyRangeId));
                var recordCounts = await Task.WhenAll(tasks);

                storageCollection.UpdateContent(destinationTimeStamp.Value);

                return recordCounts.Sum();
            }
        }

        private async Task<long> BackupPartitionContentAsync(
            IPartitionFacade partitionFacade,
            IStoragePartitionController storagePartitionController,
            long? lastContentTimeStamp,
            ILogger logger)
        {
            logger.WriteEvent("Backup-Start-Partition");

            var partitionPathParts = partitionFacade.Parent.PartitionPath.Split('/').Skip(1);
            var feed = partitionFacade.GetChangeFeed(lastContentTimeStamp);
            var metaStream = new MemoryStream();
            var contentStream = new MemoryStream();
            var pendingStorageTask = Task.CompletedTask;
            var batchId = 0;
            var recordCount = (long)0;

            while (feed.HasMoreResults)
            {
                var batch = await feed.GetBatchAsync();
                var batchLogger = logger.AddContext("batch", batchId.ToString());

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

                batchLogger
                    .AddContext("count", batch.Count)
                    .WriteEvent("Backup-End-Partition-Batch-Count");
                batchLogger
                    .AddContext("count", metaStream.Length)
                    .WriteEvent("Backup-End-Partition-Batch-MetaSize");
                batchLogger
                    .AddContext("count", contentStream.Length)
                    .WriteEvent("Backup-End-Partition-Batch-ContentSize");
                pendingStorageTask = storagePartitionController.WriteBatchAsync(
                    metaStream,
                    contentStream);
            }
            await pendingStorageTask;
            logger.WriteEvent("Backup-End-Partition");

            return recordCount;
        }

        private async Task<long?> GetDestinationTimeStampAsync(
            ICollectionFacade collectionFacade,
            IStorageCollectionController storageCollection)
        {
            var lastRecordTimeStamp = await collectionFacade.GetLastUpdateTimeAsync();

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