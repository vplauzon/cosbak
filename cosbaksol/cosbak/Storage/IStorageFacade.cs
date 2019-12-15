using System;
using System.Collections.Immutable;
using System.IO;
using System.Threading.Tasks;

namespace Cosbak.Storage
{
    internal interface IStorageFacade
    {
        IStorageFacade ChangeFolder(string subFolder);

        Task<bool> DoesExistAsync(string contentPath);

        Task CreateAppendBlobAsync(string blobPath);

        Task AppendBlobAsync(string blobPath, Stream contentStream);

        Task<BlobLease?> GetLeaseAsync(string contentPath);

        Task DeleteBlobAsync(string path, BlobLease? lease);

        Task<int> DownloadRangeAsync(
            string path,
            byte[] buffer,
            long? blobOffset = null,
            long? length = null);
        
        Task CreateEmptyBlockBlobAsync(string blobName);

        Task<IImmutableList<BlockItem>> GetBlocksAsync(string blobPath);
        
        Task WriteBlockAsync(
            string blobPath,
            string blockName,
            byte[] buffer,
            int length,
            BlobLease? lease);

        void WriteAsync(
            string blobPath,
            IImmutableList<string> blockNames,
            BlobLease? lease);
    }
}