using System;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface ICollectionFacade
    {
        IDatabaseFacade Parent { get; }

        string CollectionName { get; }

        string PartitionPath { get; }

        Task<(long currentTimeStamp, int count, long maxTimeStamp)> SizeTimeWindowAsync(
            long minTimeStamp,
            int maxItemCount);

        StreamIterator GetTimeWindowDocuments(long minTimeStamp, long maxTimeStamp);

        StreamIterator GetAllIds();
    }
}