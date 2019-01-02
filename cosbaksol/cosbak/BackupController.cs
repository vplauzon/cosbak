using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Threading.Tasks;
using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;

namespace Cosbak
{
    public class BackupController
    {
        private const int DEFAULT_RAM = 20;

        private readonly IImmutableList<ICosmosDbAccountGateway> _cosmosDbGateways;
        private readonly IStorageGateway _storageGateway;
        private readonly int _ram;

        public BackupController(
            IEnumerable<ICosmosDbAccountGateway> cosmosDbGateways,
            IStorageGateway storageGateway,
            int? ram)
        {
            if (cosmosDbGateways == null)
            {
                throw new ArgumentNullException(nameof(cosmosDbGateways));
            }
            _cosmosDbGateways = ImmutableArray<ICosmosDbAccountGateway>.Empty.AddRange(cosmosDbGateways);
            _storageGateway = storageGateway ?? throw new ArgumentNullException(nameof(storageGateway));
            _ram = ram == null
                ? DEFAULT_RAM
                : ram.Value;
        }

        public async Task BackupAsync()
        {
            foreach (var cosmosAccount in _cosmosDbGateways)
            {
                foreach (var db in await cosmosAccount.GetDatabasesAsync())
                {
                    foreach (var collection in await db.GetCollectionsAsync())
                    {
                        await BackupCollectionAsync(collection);
                    }
                }
            }
        }

        private async Task BackupCollectionAsync(ICollectionGateway collection)
        {
            var partitionList = await collection.GetPartitionsAsync();
            var account = collection.Parent.Parent.AccountName;
            var db = collection.Parent.DatabaseName;
            var blobPrefix = $"{account}/{db}/{collection.CollectionName}/backups/0/";

            foreach (var partition in partitionList)
            {
                var feed = partition.GetChangeFeed();
                var indexPath = blobPrefix + partition.KeyRangeId + ".index";
                var contentPath = blobPrefix + partition.KeyRangeId + ".content";

                await Task.WhenAll(
                    _storageGateway.CreateBlobAsync(indexPath),
                    _storageGateway.CreateBlobAsync(contentPath));
                while (feed.HasMoreResults)
                {
                    var batch = await feed.GetBatchAsync();
                    var indexStream = new MemoryStream();
                    var contentStream = new MemoryStream();

                    foreach (var doc in batch)
                    {
                        doc.MetaData.WriteAsync(indexStream);
                        doc.WriteContentAsync(contentStream);
                    }

                    indexStream.Position = 0;
                    contentStream.Position = 0;
                    await Task.WhenAll(
                        _storageGateway.AppendBlobAsync(indexPath, indexStream),
                        _storageGateway.AppendBlobAsync(contentPath, contentStream));
                }
            }
        }
    }
}