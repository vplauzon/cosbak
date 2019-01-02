using System.Collections.Generic;

namespace Cosbak.Cosmos
{
    public interface IPartitionGateway
    {
        IAsyncStream<DocumentObject> GetChangeFeed();
    }
}