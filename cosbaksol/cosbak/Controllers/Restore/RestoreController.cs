using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Restore
{
    internal class RestoreController
    {
        private readonly ILogger _logger;
        private readonly IStorageFacade _storageFacade;
        private readonly ICollectionFacade _collectionFacade;
        private readonly ReadonlyIndexFile _indexFile;

        public RestoreController(
            string sourceAccount,
            string sourceDb,
            string sourceCollection,
            IStorageFacade storageFacade,
            ICollectionFacade collectionFacade,
            ILogger logger)
        {
            _storageFacade = storageFacade;
            _collectionFacade = collectionFacade;
            _logger = logger;
            _indexFile = new ReadonlyIndexFile(
                storageFacade,
                sourceAccount,
                sourceDb,
                sourceCollection,
                logger);
        }

        public async Task InitializeAsync()
        {
            await _indexFile.InitializeAsync();
        }

        public async Task RestoreAsync()
        {
            await Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            await _indexFile.DisposeAsync();
        }
    }
}