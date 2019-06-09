using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public interface ICollectionBackupController
    {
        string Account { get; }

        string Database { get; }

        string Collection { get; }

        Task<IEnumerable<IBatchBackupController>> GetBatchesAsync();
    }
}