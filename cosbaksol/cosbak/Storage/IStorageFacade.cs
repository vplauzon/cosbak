using System;
using System.IO;
using System.Threading.Tasks;

namespace Cosbak.Storage
{
    public interface IStorageFacade
    {
        IStorageFacade ChangeFolder(string subFolder);

        Task<bool> DoesExistAsync(string contentPath);

        Task CreateAppendBlobAsync(string blobPath);

        Task AppendBlobAsync(string blobPath, Stream contentStream);

        Task<BlobLease?> GetLeaseAsync(string contentPath);

        Task DeleteBlobAsync(string path);

        Task<int> DownloadRangeAsync(
            string path,
            byte[] buffer,
            long? blobOffset = null,
            long? length = null);
        
        Task CreateEmptyBlockBlobAsync(string blobName);
    }
}