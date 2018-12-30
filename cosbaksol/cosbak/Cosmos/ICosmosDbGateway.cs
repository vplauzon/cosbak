using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface ICosmosDbGateway
    {
        Task<IEnumerable<IDatabaseGateway>> GetDatabasesAsync();
    }
}