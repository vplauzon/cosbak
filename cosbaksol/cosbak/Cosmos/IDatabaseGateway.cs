using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface IDatabaseGateway
    {
        ICosmosDbAccountGateway Parent { get; }

        string DatabaseName { get; }

        Task<IEnumerable<ICollectionGateway>> GetCollectionsAsync();
    }
}