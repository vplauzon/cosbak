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
                var context = ImmutableDictionary<string, string>
                    .Empty
                    .Add("account", plan.Collection.Parent.Parent.AccountName)
                    .Add("db", plan.Collection.Parent.DatabaseName)
                    .Add("collection", plan.Collection.CollectionName);
                var cosmosCollection = new CosmosCollectionController(plan.Collection, _logger)
                    as ICosmosCollectionController;

                _logger.WriteEvent("Backup-Start-Collection", context);
                _logger.Display($"Collection {cosmosCollection.Account}"
                    + $".{cosmosCollection.Database}.{cosmosCollection.Collection}");

                var storageCollection = await _storageController.LockLogBlobAsync(
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