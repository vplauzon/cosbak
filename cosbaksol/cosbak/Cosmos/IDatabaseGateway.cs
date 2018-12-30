using System.Collections.Generic;

namespace Cosbak.Cosmos
{
    public interface IDatabaseGateway
    {
        string Name { get; }

        IEnumerable<ICollectionGateway> GetCollectionsAsync();
    }
}