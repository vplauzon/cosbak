using System.Collections.Generic;

namespace Cosbak.Cosmos
{
    public interface IPartitionGateway
    {
        string KeyRangeId { get; }

        IAsyncStream<DocumentPackage> GetChangeFeed();
    }
}