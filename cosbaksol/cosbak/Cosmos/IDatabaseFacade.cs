using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface IDatabaseFacade
    {
        ICosmosAccountFacade Parent { get; }

        string DatabaseName { get; }

        Task<IEnumerable<ICollectionFacade>> GetCollectionsAsync();
    }
}