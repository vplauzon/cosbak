using System;
using System.Collections.Generic;
using System.Text;
using Cosbak.Cosmos;

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
    }
}