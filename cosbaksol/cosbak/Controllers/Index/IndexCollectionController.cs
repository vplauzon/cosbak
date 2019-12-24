using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    internal class IndexCollectionController
    {
        #region Inner Types
        private class Initialized
        {
            public Initialized(
                IndexFile indexFile,
                ReadonlyLogFile logFile)
            {
                IndexFile = indexFile;
                LogFile = logFile;
            }

            public IndexFile IndexFile { get; }

            public ReadonlyLogFile LogFile { get; }
        }
        #endregion

        private readonly ICollectionFacade _collection;
        private readonly IImmutableList<string> _partitionParts;
        private readonly ILogger _logger;
        private readonly IStorageFacade _storageFacade;
        private readonly int _retentionInDays;
        private readonly IndexConstants _indexConstants;
        private Initialized? _initialized;

        public IndexCollectionController(
            ICollectionFacade collectionFacade,
            IStorageFacade storageFacade,
            int retentionInDays,
            IndexConstants indexConstants,
            ILogger logger)
        {
            _collection = collectionFacade;
            _partitionParts = collectionFacade
                .PartitionPath
                .Split('/')
                .Skip(1)
                .ToImmutableArray();
            _storageFacade = storageFacade;
            _retentionInDays = retentionInDays;
            _indexConstants = indexConstants;
            _logger = logger;
        }

        public async Task InitializeAsync()
        {
            if (_initialized != null)
            {
                throw new InvalidOperationException("InitializeAsync has already been called");
            }

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

            await Task.WhenAll(
                indexFile.InitializeAsync(),
                logFile.InitializeAsync());

            _initialized = new Initialized(indexFile, logFile);
        }

        public async Task DisposeAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            await Task.WhenAll(
                _initialized.IndexFile.DisposeAsync(),
                _initialized.LogFile.DisposeAsync());
        }

        public async Task<long> LoadAsync(bool needCheckpointPurge)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            _logger.Display(
                $"Index {_collection.Parent.DatabaseName}.{_collection.CollectionName}...");
            _logger.WriteEvent("Index-Collection-Start");

            await LoadDocumentsAsync();
            await LoadStoredProceduresAsync();
            await _initialized.IndexFile.PersistAsync();
            _logger.WriteEvent("Index-Collection-End");

            return _initialized.LogFile.LastTimeStamp;
        }

        private async Task LoadStoredProceduresAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            _logger.Display("Index Stored Procedures");
            _logger.WriteEvent("Index-Collection-Sprocs-Start");
            await Task.CompletedTask;
            _logger.WriteEvent("Index-Collection-Sprocs-End");
        }

        private async Task LoadDocumentsAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            _logger.Display("Index documents");
            _logger.WriteEvent("Index-Collection-Documents-Start");

            var bufferSizes = GetDocumentBufferSizes();

            using (var indexBuffer = BufferPool.Rent(bufferSizes.indexSize))
            using (var contentBuffer = BufferPool.Rent(bufferSizes.contentSize))
            using (var indexStream = new MemoryStream(indexBuffer.Buffer))
            using (var contentStream = new MemoryStream(contentBuffer.Buffer))
            {
                long lastTimeStamp = 0;

                await foreach (var item in _initialized.LogFile.ReadDocumentsAsync(
                    _initialized.IndexFile.LastDocumentTimeStamp,
                    _indexConstants.MaxLogBufferSize))
                {
                    var (metaData, content) = SplitDocument(item.doc);

                    lastTimeStamp = item.batchTimeStamp;
                    if (!HasCapacity(indexStream, contentStream, metaData))
                    {
                        await _initialized.IndexFile.PushDocumentsAsync(
                            indexBuffer.Buffer,
                            indexStream.Position,
                            contentBuffer.Buffer,
                            contentStream.Position);
                        indexStream.Position = 0;
                        contentStream.Position = 0;
                    }
                    metaData.Write(indexStream);
                    contentStream.Write(content);
                }
                await _initialized.IndexFile.PushDocumentsAsync(
                    indexBuffer.Buffer,
                    indexStream.Position,
                    contentBuffer.Buffer,
                    contentStream.Position);

                _logger.WriteEvent("Index-Collection-Documents-End");
            }
        }

        private (int indexSize, int contentSize) GetDocumentBufferSizes()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var logSize = _initialized.LogFile.GetDocumentLogSize(
                _initialized.IndexFile.LastDocumentTimeStamp);
            var indexSize = (int)Math.Min(
                logSize / (_indexConstants.MaxContentBufferSize / _indexConstants.MaxIndexBufferSize),
                (long)_indexConstants.MaxIndexBufferSize);
            var contentSize = (int)Math.Min(
                logSize,
                (long)_indexConstants.MaxContentBufferSize);

            return (indexSize, contentSize);
        }

        private static bool HasCapacity(
            Stream indexStream,
            Stream contentStream,
            DocumentMetaData metaData)
        {
            var indexSpace = indexStream.Length - indexStream.Position;
            var contentSpace = contentStream.Length - contentStream.Position;

            return indexSpace >= metaData.GetBinarySize()
                && contentSpace >= metaData.ContentSize;
        }

        private (DocumentMetaData metaData, byte[] content) SplitDocument(JsonElement doc)
        {
            var id = doc.GetProperty(Constants.ID_FIELD).GetString();
            var partitionKey = GetPartitionKey(doc);
            var timeStamp = doc.GetProperty(Constants.TIMESTAMP_FIELD).GetInt64();
            var content = JsonSerializer.SerializeToUtf8Bytes(CleanDocument(doc));
            var metaData = new DocumentMetaData(
                id,
                partitionKey,
                timeStamp,
                content.Length);

            return (metaData, content);
        }

        private object? GetPartitionKey(JsonElement doc)
        {
            JsonElement current = doc;

            for (int i = 0; i != _partitionParts.Count; ++i)
            {
                var part = _partitionParts[i];
                JsonElement output;
                var hasProperty = doc.TryGetProperty(part, out output);

                if (!hasProperty || i == _partitionParts.Count - 1)
                {
                    return output;
                }
                else
                {
                    current = (JsonElement)output;
                }
            }

            throw new NotSupportedException("We should never reach this code path");
        }

        private IImmutableDictionary<string, JsonElement> CleanDocument(JsonElement doc)
        {
            var keptProperties = from property in doc.EnumerateObject()
                                 where property.Name != Constants.ID_FIELD
                                 && property.Name != "_rid"
                                 && property.Name != "_self"
                                 && property.Name != "_etag"
                                 && property.Name != "_attachments"
                                 select KeyValuePair.Create(property.Name, property.Value);

            return ImmutableDictionary.CreateRange(keptProperties);
        }
    }
}