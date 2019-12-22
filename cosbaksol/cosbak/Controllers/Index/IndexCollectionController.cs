using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    internal class IndexCollectionController
    {
        #region Inner Types
        private class IndexIterationController
        {
            #region Inner Types
            private class LoggedDocuments
            {
                public IDictionary<string, object>[] Documents { get; set; } =
                    new IDictionary<string, object>[0];
            }
            #endregion

            private readonly IndexFile _indexFile;
            private readonly ReadonlyLogFile _logFile;
            private readonly IndexConstants _indexConstants;

            public IndexIterationController(
                IndexFile indexFile,
                ReadonlyLogFile logFile,
                IndexConstants indexConstants)
            {
                _indexFile = indexFile;
                _logFile = logFile;
                _indexConstants = indexConstants;
            }

            public async Task IndexAsync(bool needCheckpointPurge)
            {
                await IndexDocumentsAsync();
            }

            private async Task IndexDocumentsAsync()
            {
                var enumerable = _logFile.LoadDocumentBufferAsync(
                    _indexFile.LastDocumentTimeStamp,
                    _indexConstants.MaxLogBufferSize);

                await foreach (var logBuffer in enumerable)
                {
                    foreach (var buffer in logBuffer.Buffers)
                    {
                        var logged = JsonSerializer.Deserialize<LoggedDocuments>(buffer.Span);

                        int a = 3;

                        ++a;
                        //await _indexFile.PersistAsync();
                    }
                }
            }
        }
        #endregion

        private readonly ICollectionFacade _collection;
        private readonly ILogger _logger;
        private readonly IStorageFacade _storageFacade;
        private readonly int _retentionInDays;
        private readonly IndexConstants _indexConstants;

        public IndexCollectionController(
            ICollectionFacade collectionFacade,
            IStorageFacade storageFacade,
            int retentionInDays,
            IndexConstants indexConstants,
            ILogger logger)
        {
            _collection = collectionFacade;
            _storageFacade = storageFacade;
            _retentionInDays = retentionInDays;
            _indexConstants = indexConstants;
            _logger = logger;
        }

        public async Task IndexAsync(bool needCheckpointPurge)
        {
            _logger.Display(
                $"Index {_collection.Parent.DatabaseName}.{_collection.CollectionName}...");
            _logger.WriteEvent("Index-Collection-Start");

            var indexFile = new IndexFile(
                _storageFacade,
                _collection.Parent.Parent.AccountName,
                _collection.Parent.DatabaseName,
                _collection.CollectionName,
                _logger);
            var logFile = new ReadonlyLogFile(
                _storageFacade,
                _collection.Parent.Parent.AccountName,
                _collection.Parent.DatabaseName,
                _collection.CollectionName,
                _logger);

            await indexFile.InitializeAsync();

            try
            {
                await logFile.InitializeAsync();
                try
                {
                    var subController = new IndexIterationController(
                        indexFile,
                        logFile,
                        _indexConstants);

                    await subController.IndexAsync(needCheckpointPurge);
                    _logger.WriteEvent("Index-Collection-End");
                }
                finally
                {
                    await logFile.DisposeAsync();
                }
            }
            finally
            {
                await indexFile.DisposeAsync();
            }
        }
    }
}