using Cosbak.Cosmos;
using Newtonsoft.Json.Linq;

namespace Cosbak.Controllers.Backup
{
    public interface ICosmosPartitionController
    {
        string Id { get; }

        string PartitionPath { get; }

        IAsyncStream<JObject> GetChangeFeed(long? lastContentTimeStamp);
    }
}