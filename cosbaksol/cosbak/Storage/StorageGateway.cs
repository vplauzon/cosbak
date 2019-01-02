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

        public StorageGateway(string accountName, string container, string token)
        {
            var client = new CloudBlobClient(
                new Uri($"https://{accountName}.blob.core.windows.net"),
                new StorageCredentials(token));

            _container = client.GetContainerReference(container);
        }

        async Task IStorageGateway.CreateBlobAsync(string appendBlobPath)
        {
            var blob = _container.GetAppendBlobReference(appendBlobPath);

            await blob.CreateOrReplaceAsync();
        }

        async Task IStorageGateway.AppendBlobAsync(string appendBlobPath, Stream contentStream)
        {
            var blob = _container.GetAppendBlobReference(appendBlobPath);

            await blob.AppendFromStreamAsync(contentStream);
        }
    }
}