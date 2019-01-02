using System.Threading.Tasks;

namespace Cosbak.Storage
{
    public interface IStorageGateway
    {
        Task CreateBlobAsync(string appendBlobPath);

        Task AppendBlobContentAsync(string appendBlobPath, string content);
    }
}