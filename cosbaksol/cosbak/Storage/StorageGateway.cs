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

        async Task IStorageGateway.CreateBlobAsync(string appendBlobPath)
        {
            var blob = _container.GetAppendBlobReference(_blobPrefix + appendBlobPath);

            await blob.CreateOrReplaceAsync();
        }

        async Task IStorageGateway.AppendBlobAsync(string appendBlobPath, Stream contentStream)
        {
            var blob = _container.GetAppendBlobReference(_blobPrefix + appendBlobPath);

            await blob.AppendFromStreamAsync(contentStream);
        }
    }
}