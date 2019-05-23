﻿using System;
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
        private static readonly TimeSpan WAIT_FOR_CURRENT_BACKUP_PERIOD = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan WAIT_FOR_CURRENT_BACKUP_TOTAL = TimeSpan.FromSeconds(15);

        private readonly TelemetryClient _telemetry;
        private readonly ICosmosDbAccountGateway _cosmosDbGateway;
        private readonly IStorageGateway _storageGateway;

        public BackupController(
            TelemetryClient telemetry,
            ICosmosDbAccountGateway cosmosDbGateway,
            IStorageGateway storageGateway)
        {
            _telemetry = telemetry;
            _cosmosDbGateway = cosmosDbGateway ?? throw new ArgumentNullException(nameof(cosmosDbGateway));
            _storageGateway = storageGateway ?? throw new ArgumentNullException(nameof(storageGateway));
        }

        public async Task BackupAsync()
        {
            var accountProperties = ImmutableDictionary<string, string>
                .Empty
                .Add("account", _cosmosDbGateway.AccountName);

            TrackEvent("Backup-Start", accountProperties);
            Console.WriteLine($"Account:  {_cosmosDbGateway.AccountName}");
            foreach (var db in await _cosmosDbGateway.GetDatabasesAsync())
            {
                var dbProperties = accountProperties.Add("db", db.DatabaseName);

                Console.WriteLine($"Db:  {db.DatabaseName}");
                TrackEvent("Backup-Start-Db", dbProperties);
                foreach (var collection in await db.GetCollectionsAsync())
                {
                    var collectionProperties =
                        dbProperties.Add("collection", collection.CollectionName);

                    Console.WriteLine($"Collection:  {collection.CollectionName}");
                    TrackEvent("Backup-Start-Collection", collectionProperties);

                    var toTimeStamp = await BackupCollectionAsync(1, collection, collectionProperties);

                    TrackEvent(
                        "Backup-End-Collection",
                        collectionProperties.Add("time", toTimeStamp.ToString()));
                }
                TrackEvent("Backup-End-Db", dbProperties);
            }
            TrackEvent("Backup-End", accountProperties);
        }

        private void TrackEvent(
            string eventName,
            IImmutableDictionary<string, string> properties = null,
            IImmutableDictionary<string, double> metrics = null)
        {
            _telemetry.TrackEvent(
                eventName,
                properties == null ? null : properties.ToDictionary(p => p.Key, p => p.Value),
                metrics == null ? null : metrics.ToDictionary(p => p.Key, p => p.Value));
        }

        private async Task<long?> BackupCollectionAsync(
            int rotation,
            ICollectionGateway collection,
            IImmutableDictionary<string, string> collectionProperties)
        {
            var db = collection.Parent.DatabaseName;
            var backupPrefix = $"{rotation.ToString("D10")}/{db}.{collection.CollectionName}/backups/";
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

                if (currentBackup != null && currentBackup.FromTimeStamp != currentBackup.ToTimeStamp)
                {
                    //  Max long is 9223372036854775807, hence 19 digits
                    var blobPrefix =
                        await PickContentFolderAsync($"{backupPrefix}{currentBackup.ToTimeStamp.ToString("D19")}") + '/';
                    //  Ensures a folder is created as fast as possible to avoid clashes
                    var startedMarkerTask = _storageGateway.UploadBlockBlobAsync(blobPrefix + "started", string.Empty);
                    var partitionList = await collection.GetPartitionsAsync();

                    Console.WriteLine($"{partitionList.Length} partitions");
                    //  Run all partitions in parallel to take advantage of Cosmos Compute
                    await Task.WhenAll(from partition in partitionList
                                       select BackupPartitionAsync(blobPrefix, partition, collectionProperties));

                    var doneMarkerTask = _storageGateway.UploadBlockBlobAsync(blobPrefix + "done", string.Empty);
                    if (currentBackupLease != null)
                    {   //  Marker blob stating the backup was fully completed
                        await _storageGateway.UploadBlockBlobAsync(lastBackupPath, currentBackup.ToTimeStamp.ToString("D19"));
                    }
                    await Task.WhenAll(startedMarkerTask, doneMarkerTask);
                }
                else
                {
                    TrackEvent("Backup-No Backup required", collectionProperties);
                }
                if (currentBackupLease != null)
                {
                    await currentBackupLease.ReleaseLeaseAsync();
                }

                return currentBackup == null ? (long?)null : currentBackup.ToTimeStamp;
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

                if (lastUpdateTime != null)
                {
                    var currentBackup = new CurrentBackup
                    {
                        FromTimeStamp = lastBackupTime,
                        ToTimeStamp = lastUpdateTime.Value
                    };
                    var currentBackupContent = JsonConvert.SerializeObject(currentBackup);

                    await _storageGateway.UploadBlockBlobAsync(currentBackupPath, currentBackupContent, currentBackupLease.LeaseId);

                    return currentBackup;
                }
                else
                {
                    return null;
                }
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

            throw new CosbakException("Master backup process didn't materialize backup parameters");
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

        private async Task BackupPartitionAsync(
            string blobPrefix,
            IPartitionGateway partition,
            IImmutableDictionary<string, string> collectionProperties)
        {
            var partitionProperties = collectionProperties.Add("partition", partition.KeyRangeId);

            TrackEvent("Backup-Start-Partition", partitionProperties);

            var feed = partition.GetChangeFeed();
            var indexPath = blobPrefix + partition.KeyRangeId + ".index";
            var contentPath = blobPrefix + partition.KeyRangeId + ".content";
            var pendingStorageTask = Task.WhenAll(
                _storageGateway.CreateAppendBlobAsync(indexPath),
                _storageGateway.CreateAppendBlobAsync(contentPath));
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
                    _storageGateway.AppendBlobAsync(indexPath, indexStream),
                    _storageGateway.AppendBlobAsync(contentPath, contentStream));
            }
            //  Make sure storage work is done
            await pendingStorageTask;

            var metrics = ImmutableDictionary<string, double>.Empty
                .Add("totalIndex", totalIndex)
                .Add("totalContent", totalContent)
                .Add("totalDocuments", totalDocuments);

            TrackEvent("Backup-End-Partition", partitionProperties, metrics);
        }
    }
}