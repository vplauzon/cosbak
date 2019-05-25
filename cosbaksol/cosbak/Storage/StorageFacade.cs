using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
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
            string folder)
        {
            var storageUri = new Uri($"https://{accountName}.blob.core.windows.net/");
            var client = new CloudBlobClient(storageUri, credentials);

            _container = client.GetContainerReference(container);
            _blobPrefix = folder;
        }
        #endregion

        async Task<string> IStorageFacade.GetContentAsync(string contentPath)
        {
            var blob = _container.GetBlobReference(_blobPrefix + contentPath);

            using (var stream = new MemoryStream())
            {
                await blob.DownloadToStreamAsync(stream);

                stream.Position = 0;
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
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

        async Task IStorageFacade.UploadBlockBlobAsync(string blobPath, string content, string leaseId)
        {
            var blob = _container.GetBlockBlobReference(_blobPrefix + blobPath);

            await blob.UploadTextAsync(
                content,
                new AccessCondition
                {
                    LeaseId = leaseId
                },
                null,
                null);
        }

        async Task IStorageFacade.AppendBlobAsync(string blobPath, Stream contentStream)
        {
            var blob = _container.GetAppendBlobReference(_blobPrefix + blobPath);

            await blob.AppendFromStreamAsync(contentStream);
        }

        async Task<BlobLease> IStorageFacade.GetLeaseAsync(string blobPath)
        {
            var blob = _container.GetBlobReference(_blobPrefix + blobPath);

            return await BlobLease.CreateLeaseAsync(blob);
        }

        async Task<string[]> IStorageFacade.ListBlobsAsync(string blobPrefix)
        {
            var prefix = _blobPrefix + blobPrefix;
            var list = new List<string>();
            BlobContinuationToken token = null;

            do
            {
                var segment = await _container.ListBlobsSegmentedAsync(
                    blobPrefix,
                    true,
                    BlobListingDetails.None,
                    null,
                    token,
                    null,
                    null);
                var paths = from i in segment.Results
                            let cloudBlob = (CloudBlob)i
                            let fullPath = cloudBlob.Name.Substring(_blobPrefix.Length)
                            select fullPath;

                list.AddRange(paths);
                token = segment.ContinuationToken;
            }
            while (token != null);

            return list.ToArray();
        }
    }
}