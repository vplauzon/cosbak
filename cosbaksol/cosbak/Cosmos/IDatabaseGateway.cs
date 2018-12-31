using System.Collections.Generic;

namespace Cosbak.Cosmos
{
    public interface IDatabaseGateway
    {
        ICosmosDbAccountGateway Parent { get; }

        string DatabaseName { get; }

        IEnumerable<ICollectionGateway> GetCollectionsAsync();
    }
}