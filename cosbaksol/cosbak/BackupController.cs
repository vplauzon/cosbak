using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using Microsoft.ApplicationInsights;

namespace Cosbak
{
    public class BackupController
    {
        private const int DEFAULT_RAM = 20;

        private readonly TelemetryClient _telemetry;
        private readonly IImmutableList<ICosmosDbAccountGateway> _cosmosDbGateways;
        private readonly IStorageGateway _storageGateway;
        private readonly int _ram;

        public BackupController(
            TelemetryClient telemetry,
            IEnumerable<ICosmosDbAccountGateway> cosmosDbGateways,
            IStorageGateway storageGateway,
            int? ram)
        {
            if (cosmosDbGateways == null)
            {
                throw new ArgumentNullException(nameof(cosmosDbGateways));
            }
            _telemetry = telemetry;
            _cosmosDbGateways = ImmutableArray<ICosmosDbAccountGateway>.Empty.AddRange(cosmosDbGateways);
            _storageGateway = storageGateway ?? throw new ArgumentNullException(nameof(storageGateway));
            _ram = ram == null
                ? DEFAULT_RAM
                : ram.Value;
        }

        public async Task BackupAsync()
        {
            _telemetry.TrackEvent("Backup-Start");
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
            var account = collection.Parent.Parent.AccountName;
            var db = collection.Parent.DatabaseName;
            var backupPrefix = $"{account}/{db}/{collection.CollectionName}/backups/";
            var lastBackupPath = backupPrefix + "lastBackup";
            var lastBackupTime = await GetLastBackupTimeAsync(lastBackupPath);
            var lastUpdateTime = await collection.GetLastUpdateTimeAsync();

            if (lastUpdateTime != lastBackupTime)
            {
                var blobPrefix = $"{backupPrefix}/{lastUpdateTime}/";
                var partitionList = await collection.GetPartitionsAsync();

                foreach (var partition in partitionList)
                {
                    await BackupPartitionAsync(blobPrefix, partition);
                }
            }
            else
            {
                _telemetry.TrackEvent("Backup-No Backup required");
            }
        }

        private async Task<long?> GetLastBackupTimeAsync(string lastBackupPath)
        {
            if (await _storageGateway.DoesExistAsync(lastBackupPath))
            {
                var text = await _storageGateway.GetContentAsync(lastBackupPath);
                var lastUpdateTime = int.Parse(text);

                return lastUpdateTime;
            }
            else
            {
                return null;
            }
        }

        private async Task BackupPartitionAsync(string blobPrefix, IPartitionGateway partition)
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

                using (var writer = new BinaryWriter(indexStream, Encoding.ASCII, true))
                {
                    foreach (var doc in batch)
                    {
                        doc.MetaData.WriteAsync(writer);
                        contentStream.Write(doc.Content);
                    }
                }

                indexStream.Flush();
                contentStream.Flush();
                indexStream.Position = 0;
                contentStream.Position = 0;
                await Task.WhenAll(
                    _storageGateway.AppendBlobAsync(indexPath, indexStream),
                    _storageGateway.AppendBlobAsync(contentPath, contentStream));
            }
        }
    }
}