using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public interface IBatchBackupController
    {
        int FolderId { get; }

        long TimeStamp { get; }

        Task<IImmutableList<IPartitionBackupController>> GetPartitionsAsync();
    }
}