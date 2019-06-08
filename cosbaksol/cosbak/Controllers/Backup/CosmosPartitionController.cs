using System;
using System.Collections.Generic;
using System.Text;
using Cosbak.Cosmos;
using Newtonsoft.Json.Linq;

namespace Cosbak.Controllers.Backup
{
    public class CosmosPartitionController : ICosmosPartitionController
    {
        private IPartitionFacade _partition;
        private ILogger _logger;

        public CosmosPartitionController(IPartitionFacade partition, ILogger logger)
        {
            _partition = partition;
            _logger = logger;
        }

        string ICosmosPartitionController.Id => _partition.KeyRangeId;

        string ICosmosPartitionController.PartitionPath => _partition.Parent.PartitionPath;

        IAsyncStream<JObject> ICosmosPartitionController.GetChangeFeed(long? lastContentTimeStamp)
            => _partition.GetChangeFeed(lastContentTimeStamp);
    }
}