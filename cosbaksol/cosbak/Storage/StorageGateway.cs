using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Cosbak.Storage
{
    internal class StorageGateway : IStorageGateway
    {
        private readonly CloudBlobContainer _container;
        private readonly string _blobPrefix;

        public StorageGateway(
            string accountName,
            string container,
            string token,
            string blobPrefix)
        {
            var client = new CloudBlobClient(
                new Uri($"https://{accountName}.blob.core.windows.net"),
                new StorageCredentials(token));

            _container = client.GetContainerReference(container);
            _blobPrefix = (string.IsNullOrWhiteSpace(blobPrefix) ? "" : blobPrefix + '/');
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

        async Task IStorageGateway.UploadBlockBlobAsync(string blobPath, string content)
        {
            var blob = _container.GetBlockBlobReference(_blobPrefix + blobPath);

            await blob.UploadTextAsync(content);
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
    }
}