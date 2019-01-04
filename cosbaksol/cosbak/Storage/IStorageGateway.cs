using System.IO;
using System.Threading.Tasks;

namespace Cosbak.Storage
{
    public interface IStorageGateway
    {
        Task<bool> DoesExistAsync(string contentPath);

        Task CreateBlobAsync(string appendBlobPath);

        Task AppendBlobAsync(string contentPath, Stream contentStream);

        Task<string> GetContentAsync(string contentPath);
    }
}