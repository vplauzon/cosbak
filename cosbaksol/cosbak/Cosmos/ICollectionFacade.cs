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

        Task<long?> GetLastUpdateTimeAsync(long fromTime, int maxItemCount);

        StreamIterator GetTimeWindowDocuments(long minTime, long maxTime);
    }
}