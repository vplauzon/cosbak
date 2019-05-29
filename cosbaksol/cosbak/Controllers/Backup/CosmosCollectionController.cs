﻿using Cosbak.Cosmos;

namespace Cosbak.Controllers.Backup
{
    public class CosmosCollectionController : ICosmosCollectionController
    {
        private ICollectionFacade _collection;
        private ILogger _logger;

        public CosmosCollectionController(ICollectionFacade collection, ILogger logger)
        {
            _collection = collection;
            _logger = logger;
        }

        string ICosmosCollectionController.Account => _collection.Parent.Parent.AccountName;

        string ICosmosCollectionController.Database => _collection.Parent.DatabaseName;

        string ICosmosCollectionController.Collection => _collection.CollectionName;
    }
}