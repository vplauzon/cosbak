using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IStoragePartitionController
    {
        Task WriteBatchAsync(byte[] metaBuffer, byte[] contentBuffer);
    }
}