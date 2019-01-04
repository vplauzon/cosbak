using System.IO;
using System.Threading.Tasks;

namespace Cosbak.Storage
{
    public interface IStorageGateway
    {
        Task<bool> DoesExistAsync(string contentPath);

        Task<string> GetContentAsync(string contentPath);

        Task CreateAppendBlobAsync(string blobPath);

        Task UploadBlockBlobAsync(string blobPath, string content);

        Task AppendBlobAsync(string blobPath, Stream contentStream);

        Task<BlobLease> GetLeaseAsync(string contentPath);
    }
}