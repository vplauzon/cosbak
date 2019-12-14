using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface ICosmosAccountFacade
    {
        string AccountName { get; }

        Task<IEnumerable<IDatabaseFacade>> GetDatabasesAsync();
    }
}