using System;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public interface IPartitionBackupController
    {
        string PartitionId { get; }

        Task<int> LoadIndexAsync(byte[] indexBuffer);
    }
}