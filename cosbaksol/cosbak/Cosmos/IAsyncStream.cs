using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public interface IAsyncStream<T>
    {
        bool HasMoreResults { get; }

        Task<IImmutableList<T>> GetBatchAsync();
    }
}