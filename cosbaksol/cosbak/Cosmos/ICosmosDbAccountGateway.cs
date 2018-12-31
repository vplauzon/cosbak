using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface ICosmosDbAccountGateway
    {
        string AccountName { get; }

        Task<IEnumerable<IDatabaseGateway>> GetDatabasesAsync();
    }
}