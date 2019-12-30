using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Restore
{
    internal class RestoreController
    {
        const int COSMOS_BATCH = 100;

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

        public async Task RestoreAsync(DateTime? pointInTime)
        {
            var timeStamp = pointInTime == null
                ? long.MaxValue
                : throw new NotSupportedException();

            await RestoreDocumentsAsync(timeStamp);
        }

        public async Task DisposeAsync()
        {
            await _indexFile.DisposeAsync();
        }

        private async Task RestoreDocumentsAsync(long timeStamp)
        {
            await foreach (var batch in _indexFile.ReadLatestDocumentsAsync(timeStamp))
            {
                var tasks = new List<Task>(2 * COSMOS_BATCH);

                foreach (var content in batch)
                {
                    tasks.Add(_collectionFacade.WriteDocumentAsync(content));
                    if (tasks.Count >= 2 * COSMOS_BATCH)
                    {
                        await Task.WhenAll(tasks.Take(COSMOS_BATCH));
                        tasks.RemoveRange(0, COSMOS_BATCH);
                    }
                }
                await Task.WhenAll(tasks);
            }
        }
    }
}