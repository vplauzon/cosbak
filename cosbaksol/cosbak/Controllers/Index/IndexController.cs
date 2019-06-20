using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public class IndexController
    {
        private readonly ILogger _logger;
        private readonly IIndexStorageController _indexStorageController;
        private readonly byte[] _indexBuffer = new byte[Constants.MAX_INDEX_LENGTH];

        public IndexController(ILogger logger, IIndexStorageController indexStorageController)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _indexStorageController = indexStorageController
                ?? throw new ArgumentNullException(nameof(indexStorageController));
        }

        public async Task IndexAsync()
        {
            _logger.Display("Indexing...");
            _logger.WriteEvent("Indexing-Start");

            var collections = await _indexStorageController.GetCollectionsAsync();

            foreach (var collectionController in collections)
            {
                var context = ImmutableDictionary<string, string>
                    .Empty
                    .Add("account", collectionController.Account)
                    .Add("db", collectionController.Database)
                    .Add("collection", collectionController.Collection);

                _logger.WriteEvent("Indexing-Start-Collection", context);
                _logger.Display($"Collection {collectionController.Account}"
                    + $".{collectionController.Database}.{collectionController.Collection}");

                await IndexCollectionAsync(collectionController, context);

                _logger.WriteEvent("Indexing-End-Collection", context);
            }
            _logger.WriteEvent("Indexing-End");
        }

        private async Task IndexCollectionAsync(
            ICollectionBackupController collectionController,
            IImmutableDictionary<string, string> context)
        {
            var batches = await collectionController.GetUnprocessedBatchesAsync();

            _logger.Display($"{batches.Count} batches");

            if(batches.Any())
            {
                var firstTimeStamp = batches.First().TimeStamp;
                var blobIndex =
                    await collectionController.GetCurrentBlobIndexControllerAsync(firstTimeStamp);

                foreach (var batch in batches)
                {
                    _logger.Display(
                        $"Processing batch {batch.FolderId} at timestamp {batch.TimeStamp}");
                    await IndexBatchAsync(
                        batch,
                        blobIndex,
                        context.Add("batch", batch.TimeStamp.ToString()));
                }
            }
        }

        private async Task IndexBatchAsync(
            IBatchBackupController batch,
            IBlobIndexController blobIndex,
            IImmutableDictionary<string, string> context)
        {
            var partitions = await batch.GetPartitionsAsync();

            _logger.Display($"{partitions.Count} partitions");

            foreach (var partition in partitions)
            {
                _logger.Display($"Partition {partition.PartitionId}");

                await IndexPartitionAsync(
                    partition,
                    blobIndex,
                    context.Add("partition", partition.PartitionId));
            }
        }

        private async Task IndexPartitionAsync(
            IPartitionBackupController partition,
            IBlobIndexController blobIndex,
            IImmutableDictionary<string, string> context)
        {
            var length = await partition.LoadIndexAsync(_indexBuffer);
            var index = new Memory<byte>(_indexBuffer, 0, length);

            await blobIndex.AppendAsync(index);
        }
    }
}