using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface IAsyncStream<T>
    {
        bool HasMoreResults { get; }

        Task<T[]> GetBatchAsync();
    }
}