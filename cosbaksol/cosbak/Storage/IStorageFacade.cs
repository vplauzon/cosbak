using System;
using System.Collections;
using System.Collections.Generic;
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

        Task<BlobLease?> AcquireLeaseAsync(string contentPath);

        Task DeleteBlobAsync(string path, BlobLease? lease);

        Task<int> DownloadRangeAsync(
            string path,
            byte[] buffer,
            long? blobOffset = null,
            long? length = null,
            DateTimeOffset? snapshotTime = null);

        Task CreateEmptyBlockBlobAsync(string blobName);

        Task<IImmutableList<BlockItem>> GetBlocksAsync(
            string blobPath,
            DateTimeOffset? snapshotTime = null);

        Task WriteBlockAsync(
            string blobPath,
            string blockId,
            byte[] buffer,
            int length,
            BlobLease? lease);

        Task WriteAsync(
            string blobPath,
            IEnumerable<string> blockIds,
            BlobLease? lease);

        Task<DateTimeOffset?> SnapshotAsync(string blobPath);
        
        Task ClearSnapshotsAsync(string blobName);
    }
}