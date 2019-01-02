using System.Collections.Generic;

namespace Cosbak.Cosmos
{
    public interface IPartitionGateway
    {
        IAsyncStream<IDictionary<string, object>> GetChangeFeed();
    }
}