using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public class IndexController
    {
        private readonly ILogger _logger;
        private readonly IIndexStorageController _indexStorageController;

        public IndexController(ILogger logger, IIndexStorageController indexStorageController)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _indexStorageController = indexStorageController
                ?? throw new ArgumentNullException(nameof(indexStorageController));
        }

        public async Task IndexAsync()
        {
            _logger.Display("Indexing...");
            _logger.WriteEvent("Indexing-Start");
            foreach (var collectionController in
                await _indexStorageController.GetCollectionsAsync())
            {
                var context = ImmutableDictionary<string, string>
                    .Empty
                    .Add("account", collectionController.Account)
                    .Add("db", collectionController.Database)
                    .Add("collection", collectionController.Collection);

                _logger.WriteEvent("Indexing-Start-Collection", context);
                _logger.Display($"Collection {collectionController.Account}"
                    + $".{collectionController.Database}.{collectionController.Collection}");

                await IndexCollectionAsync(collectionController, context);

                _logger.WriteEvent("Indexing-End-Collection", context);
            }
            _logger.WriteEvent("Indexing-End");
        }

        private Task IndexCollectionAsync(
            IIndexCollectionBackupController collectionController,
            IImmutableDictionary<string, string> context)
        {
            throw new NotImplementedException();
        }
    }
}