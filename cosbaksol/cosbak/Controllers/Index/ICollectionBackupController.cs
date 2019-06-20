using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public interface ICollectionBackupController
    {
        string Account { get; }

        string Database { get; }

        string Collection { get; }

        Task<IImmutableList<IBatchBackupController>> GetUnprocessedBatchesAsync();

        Task<IBlobIndexController> GetCurrentBlobIndexControllerAsync(long firstTimeStamp);
    }
}