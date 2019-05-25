using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface IDatabaseFacade
    {
        IDatabaseAccountFacade Parent { get; }

        string DatabaseName { get; }

        Task<IEnumerable<ICollectionFacade>> GetCollectionsAsync();
    }
}