using System;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface ICollectionGateway
    {
        IDatabaseGateway Parent { get; }

        string CollectionName { get; }

        string PartitionPath { get; }

        Task<IPartitionGateway[]> GetPartitionsAsync();

        Task<Int64> GetLastUpdateTimeAsync();
    }
}