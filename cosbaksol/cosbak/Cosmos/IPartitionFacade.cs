using System.Collections.Generic;

namespace Cosbak.Cosmos
{
    public interface IPartitionFacade
    {
        string KeyRangeId { get; }

        IAsyncStream<DocumentPackage> GetChangeFeed();
    }
}