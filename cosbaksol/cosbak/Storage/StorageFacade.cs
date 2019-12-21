using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Cosbak.Storage
{
    internal class StorageFacade : IStorageFacade
    {
        private readonly CloudBlobContainer _container;
        private readonly string _blobPrefix;

        #region Constructors
        public static IStorageFacade FromKey(
            string accountName,
            string container,
            string folder,
            string key)
        {
            var credentials = new StorageCredentials(accountName, key);

            return new StorageFacade(credentials, accountName, container, folder);
        }

        public static IStorageFacade FromToken(
            string accountName,
            string container,
            string folder,
            string token)
        {
            var credentials = new StorageCredentials(token);

            return new StorageFacade(credentials, accountName, container, folder);
        }

        private StorageFacade(
            StorageCredentials credentials,
            string accountName,
            string container,
            string folder) : this(CreateContainerReference(credentials, accountName, container), folder)
        {
        }

        private StorageFacade(CloudBlobContainer container, string blobPrefix)
        {
            _container = container;
            blobPrefix = blobPrefix.Trim();
            if (blobPrefix.EndsWith('/'))
            {
                throw new ArgumentException(
                    $"Can't end with a '/':  '{blobPrefix}'",
                    nameof(blobPrefix));
            }
            _blobPrefix = blobPrefix + '/';
        }

        private static CloudBlobContainer CreateContainerReference(
            StorageCredentials credentials,
            string accountName,
            string container)
        {
            var storageUri = new Uri($"https://{accountName}.blob.core.windows.net/");
            var client = new CloudBlobClient(storageUri, credentials);

            return client.GetContainerReference(container);
        }
        #endregion

        IStorageFacade IStorageFacade.ChangeFolder(string subFolder)
        {
            if (string.IsNullOrWhiteSpace(subFolder))
            {
                throw new ArgumentNullException(nameof(subFolder));
            }

            subFolder = subFolder.Trim();

            if (subFolder.StartsWith('/'))
            {
                throw new ArgumentException("Can't start with a '/'", nameof(subFolder));
            }
            if (subFolder.EndsWith('/'))
            {
                throw new ArgumentException("Can't end with a '/'", nameof(subFolder));
            }

            var newPrefix = _blobPrefix.Length != 0
                ? new StorageFacade(_container, _blobPrefix + subFolder)
                : new StorageFacade(_container, subFolder);

            return newPrefix;
        }

        async Task<bool> IStorageFacade.DoesExistAsync(string contentPath)
        {
            var blob = _container.GetBlobReference(_blobPrefix + contentPath);

            return await blob.ExistsAsync();
        }

        async Task IStorageFacade.CreateAppendBlobAsync(string blobPath)
        {
            var blob = _container.GetAppendBlobReference(_blobPrefix + blobPath);

            await blob.CreateOrReplaceAsync();
        }

        async Task IStorageFacade.AppendBlobAsync(string blobPath, Stream contentStream)
        {
            var blob = _container.GetAppendBlobReference(_blobPrefix + blobPath);

            await blob.AppendFromStreamAsync(contentStream);
        }

        async Task<BlobLease?> IStorageFacade.AcquireLeaseAsync(string blobPath)
        {
            var blob = _container.GetBlobReference(_blobPrefix + blobPath);

            return await BlobLease.AcquireLeaseAsync(blob);
        }

        async Task IStorageFacade.DeleteBlobAsync(string path, BlobLease? lease)
        {
            var blob = _container.GetBlobReference(_blobPrefix + path);

            await blob.DeleteAsync(
                DeleteSnapshotsOption.IncludeSnapshots,
                new AccessCondition() { LeaseId = lease?.LeaseId },
                null,
                null);
        }

        async Task<int> IStorageFacade.DownloadRangeAsync(
            string path,
            byte[] buffer,
            long? blobOffset,
            long? length)
        {
            var blob = _container.GetBlobReference(_blobPrefix + path);

            return await blob.DownloadRangeToByteArrayAsync(
                buffer,
                0,
                blobOffset == null ? 0 : blobOffset,
                length == null ? buffer.Length : length);
        }

        async Task IStorageFacade.CreateEmptyBlockBlobAsync(string blobPath)
        {
            var blob = _container.GetBlockBlobReference(_blobPrefix + blobPath);

            await blob.UploadTextAsync(string.Empty);
        }

        async Task<IImmutableList<BlockItem>> IStorageFacade.GetBlocksAsync(string blobPath)
        {
            var blob = _container.GetBlockBlobReference(_blobPrefix + blobPath);
            var blockList = await blob.DownloadBlockListAsync();
            var items = from i in blockList
                        select new BlockItem(i.Name, i.Length);

            return items.ToImmutableArray();
        }

        async Task IStorageFacade.WriteBlockAsync(
            string blobPath,
            string blockId,
            byte[] buffer,
            int length,
            BlobLease? lease)
        {
            var blob = _container.GetBlockBlobReference(_blobPrefix + blobPath);

            using (var stream = new MemoryStream(buffer, 0, length))
            {
                await blob.PutBlockAsync(
                    blockId,
                    stream,
                    string.Empty,
                    new AccessCondition
                    {
                        LeaseId = lease?.LeaseId
                    },
                    null,
                    null);
            }
        }

        void IStorageFacade.WriteAsync(
            string blobPath,
            IEnumerable<string> blockIds,
            BlobLease? lease)
        {
            var blob = _container.GetBlockBlobReference(_blobPrefix + blobPath);

            blob.PutBlockListAsync(
                blockIds,
                new AccessCondition
                {
                    LeaseId = lease?.LeaseId
                },
                null,
                null);
        }
    }
}