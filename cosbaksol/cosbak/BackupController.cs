using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using Microsoft.ApplicationInsights;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;

namespace Cosbak
{
    public class BackupController
    {
        private const int DEFAULT_RAM = 20;
        private static readonly TimeSpan WAIT_FOR_CURRENT_BACKUP_PERIOD = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan WAIT_FOR_CURRENT_BACKUP_TOTAL = TimeSpan.FromSeconds(15);

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
            var currentBackupPath = backupPrefix + "currentBackup.json";

            //  Winner of the current backup lease becomes the master process
            using (var currentBackupLease = await GetCurrentBackupLease(currentBackupPath))
            {
                var lastBackupPath = backupPrefix + "lastBackup.txt";
                var currentBackup = await GetCurrentBackupLeaseAsync(
                    currentBackupLease,
                    currentBackupPath,
                    lastBackupPath,
                    collection);

                if (currentBackup.FromTimeStamp != currentBackup.ToTimeStamp)
                {
                    var blobPrefix =
                        await PickContentFolderAsync($"{backupPrefix}{currentBackup.ToTimeStamp}") + '/';
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
                if (currentBackup != null)
                {
                    await currentBackupLease.ReleaseLeaseAsync();
                }
            }
        }

        private async Task<string> PickContentFolderAsync(string blobPrefix)
        {
            var i = 0;
            var suffix = string.Empty;

            while (true)
            {
                var path = blobPrefix + suffix;
                var blobs = await _storageGateway.ListBlobsAsync(path);

                if (blobs.Any())
                {
                    ++i;
                    suffix = "." + i;
                }
                else
                {
                    return path;
                }
            }
        }

        private async Task<CurrentBackup> GetCurrentBackupLeaseAsync(
            BlobLease currentBackupLease,
            string currentBackupPath,
            string lastBackupPath,
            ICollectionGateway collection)
        {
            if (currentBackupLease == null)
            {
                var currentBackup = await GetCurrentBackupAsync(currentBackupPath);

                return currentBackup;
            }
            else
            {
                var lastBackupTimeTask = GetLastBackupTimeAsync(lastBackupPath);
                var lastUpdateTimeTask = collection.GetLastUpdateTimeAsync();
                var lastBackupTime = await lastBackupTimeTask;
                var lastUpdateTime = await lastUpdateTimeTask;
                var currentBackup = new CurrentBackup
                {
                    FromTimeStamp = lastBackupTime,
                    ToTimeStamp = lastUpdateTime
                };
                var currentBackupContent = JsonConvert.SerializeObject(currentBackup);

                await _storageGateway.UploadBlockBlobAsync(currentBackupPath, currentBackupContent, currentBackupLease.LeaseId);

                return currentBackup;
            }
        }

        private async Task<CurrentBackup> GetCurrentBackupAsync(string currentBackupPath)
        {
            var start = DateTime.Now;

            while (start.Add(WAIT_FOR_CURRENT_BACKUP_TOTAL) < DateTime.Now)
            {
                var content = await _storageGateway.GetContentAsync(currentBackupPath);

                if (string.IsNullOrWhiteSpace(content))
                {
                    await Task.Delay(WAIT_FOR_CURRENT_BACKUP_PERIOD);
                }
                else
                {
                    var currentBackup = JsonConvert.DeserializeObject<CurrentBackup>(content);

                    return currentBackup;
                }
            }

            throw new BackupException("Master backup process didn't materialize backup parameters");
        }

        private async Task<BlobLease> GetCurrentBackupLease(string currentBackupPath)
        {
            try
            {
                //  Clean record if no lease ; so we never read stale data
                await _storageGateway.UploadBlockBlobAsync(currentBackupPath, string.Empty);
            }
            catch (StorageException)
            {
                return null;
            }

            return await _storageGateway.GetLeaseAsync(currentBackupPath);
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
                _storageGateway.CreateAppendBlobAsync(indexPath),
                _storageGateway.CreateAppendBlobAsync(contentPath));
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