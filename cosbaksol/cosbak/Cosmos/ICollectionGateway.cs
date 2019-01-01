using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface ICollectionGateway
    {
        IDatabaseGateway Parent { get; }

        string CollectionName { get; }

        Task<IImmutableList<IPartitionGateway>> GetPartitionsAsync();
    }
}