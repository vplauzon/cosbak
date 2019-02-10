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
    internal class StorageGateway : IStorageGateway
    {
        private readonly CloudBlobContainer _container;
        private readonly string _blobPrefix;

        private StorageGateway(
            Uri baseUri,
            string container,
            string token,
            string blobPrefix)
        {
            var credentials = new StorageCredentials(token);
            var client = new CloudBlobClient(baseUri, credentials);

            _container = client.GetContainerReference(container);
            _blobPrefix = blobPrefix;
        }

        public static IStorageGateway Create(Uri folderUri)
        {
            var baseUri = new Uri("https://" + folderUri.Host);
            var parts = folderUri.AbsolutePath.Split('/');

            if (parts.Length < 2)
            {
                throw new BackupException("Folder Uri must at least contain the container's name:  "
                    + folderUri.AbsolutePath);
            }

            var container = parts[1];
            var token = folderUri.Query;
            var blobPrefix = parts.Length == 2
                ? string.Empty
                : string.Join('/', parts.Skip(2)) + '/';

            return new StorageGateway(baseUri, container, token, blobPrefix);
        }

        async Task<string> IStorageGateway.GetContentAsync(string contentPath)
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

        async Task<bool> IStorageGateway.DoesExistAsync(string contentPath)
        {
            var blob = _container.GetBlobReference(_blobPrefix + contentPath);

            return await blob.ExistsAsync();
        }

        async Task IStorageGateway.CreateAppendBlobAsync(string blobPath)
        {
            var blob = _container.GetAppendBlobReference(_blobPrefix + blobPath);

            await blob.CreateOrReplaceAsync();
        }

        async Task IStorageGateway.UploadBlockBlobAsync(string blobPath, string content, string leaseId)
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

        async Task IStorageGateway.AppendBlobAsync(string blobPath, Stream contentStream)
        {
            var blob = _container.GetAppendBlobReference(_blobPrefix + blobPath);

            await blob.AppendFromStreamAsync(contentStream);
        }

        async Task<BlobLease> IStorageGateway.GetLeaseAsync(string blobPath)
        {
            var blob = _container.GetBlobReference(_blobPrefix + blobPath);

            return await BlobLease.CreateLeaseAsync(blob);
        }

        async Task<string[]> IStorageGateway.ListBlobsAsync(string blobPrefix)
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