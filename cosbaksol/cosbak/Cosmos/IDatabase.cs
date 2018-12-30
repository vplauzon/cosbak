using System.Collections.Generic;

namespace Cosbak.Cosmos
{
    public interface IDatabase
    {
        IEnumerable<IDocCollection> GetCollectionsAsync();
    }
}