using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    internal class IndexCollectionController
    {
        private readonly ICollectionFacade _collection;
        private readonly ILogger _logger;
        private readonly IStorageFacade _storageFacade;
        private readonly int _retentionInDays;

        public IndexCollectionController(
            ICollectionFacade collectionFacade,
            IStorageFacade storageFacade,
            int retentionInDays,
            ILogger logger)
        {
            _collection = collectionFacade;
            _storageFacade = storageFacade;
            _retentionInDays = retentionInDays;
            _logger = logger;
        }

        public async Task IndexAsync(bool needCheckpointPurge)
        {
            _logger.Display(
                $"Index {_collection.Parent.DatabaseName}.{_collection.CollectionName}...");
            _logger.WriteEvent("Index-Collection-Start");

            await Task.CompletedTask;

            _logger.WriteEvent("Index-Collection-End");

            throw new NotImplementedException();
        }
    }
}