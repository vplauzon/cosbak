using System.IO;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IStoragePartitionController
    {
        Task WriteBatchAsync(Stream metaStream, Stream contentStream);
    }
}