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
using System.Threading;
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

        private static readonly byte[] EMPTY_CONTENT = new byte[0];

        private readonly string _account;
        private readonly string _db;
        private readonly string _collection;
        private readonly ILogger _logger;
        private readonly IStorageFacade _storageFacade;
        private readonly int? _retentionInDays;
        private readonly IndexConstants _indexConstants;
        private Initialized? _initialized;

        public IndexCollectionController(
            string account,
            string db,
            string collection,
            IStorageFacade storageFacade,
            int? retentionInDays,
            IndexConstants indexConstants,
            ILogger logger)
        {
            _account = account;
            _db = db;
            _collection = collection;
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
                _account,
                _db,
                _collection,
                _logger);
            var logFile = new ReadonlyLogFile(
                _storageFacade,
                _account,
                _db,
                _collection,
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

            _logger.Display($"Index {_db}.{_collection}...");
            _logger.WriteEvent("Index-Collection-Start");

            await LoadDocumentsAsync();
            await LoadStoredProceduresAsync();
            await _initialized.IndexFile.PersistAsync();
            _logger.WriteEvent("Index-Collection-End");

            return _initialized.LogFile.LastTimeStamp;
        }

        public Task LoadUntilAsync(DateTime? pointInTime)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            throw new NotImplementedException();
        }

        private async Task LoadStoredProceduresAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            _logger.Display("Index Stored Procedures");
            _logger.WriteEvent("Index-Collection-Sprocs-Start");

            var bufferSizes = GetStoredProcedureBufferSizes();
            var sprocs = await LoadUntilNowSprocsIndexAsync();
            var batchCount = 0;
            long lastTimeStamp = 0;

            using (var buffer = new IndexingBuffer(
                bufferSizes.indexSize,
                bufferSizes.contentSize,
                _initialized.IndexFile.PushDocumentsAsync))
            {
                await foreach (var batch in _initialized.LogFile.ReadStoredProceduresAsync(
                    _initialized.IndexFile.LastStoredProcedureTimeStamp,
                    _indexConstants.MaxLogBufferSize))
                {
                    var batchIds = ImmutableHashSet<string>.Empty;

                    lastTimeStamp = batch.BatchTimeStamp;
                    ++batchCount;
                    foreach (var item in batch.Items)
                    {
                        batchIds = batchIds.Add(item.Id.Id);
                        if (!sprocs.ContainsKey(item.Id.Id)
                            || sprocs[item.Id.Id].TimeStamp < item.Id.TimeStamp)
                        {
                            var (metaData, content) = item.Split();

                            sprocs = sprocs.SetItem(item.Id.Id, metaData.Id);
                            await buffer.WriteAsync(metaData, content);
                        }
                    }
                    var deletedIds = sprocs.Keys.ToImmutableHashSet().Except(batchIds);

                    foreach (var id in deletedIds)
                    {
                        sprocs = sprocs.Remove(id);
                        await buffer.WriteAsync(new ScriptMetaData(
                            new ScriptIdentifier(id, lastTimeStamp), 0),
                            EMPTY_CONTENT);
                    }
                }
                await buffer.FlushAsync();
                _initialized.IndexFile.UpdateLastSprocTimeStamp(lastTimeStamp);

                _logger.Display($"Indexed {buffer.ItemCount} stored procedures in {batchCount} batches");
                _logger
                    .AddContext("documentCount", buffer.ItemCount)
                    .AddContext("batchCount", batchCount)
                    .WriteEvent("Index-Collection-Sprocs-End");
            }
            _logger.WriteEvent("Index-Collection-Sprocs-End");
        }

        private async Task<IImmutableDictionary<string, ScriptIdentifier>> LoadUntilNowSprocsIndexAsync()
        {
            await Task.CompletedTask;

            return ImmutableDictionary<string, ScriptIdentifier>.Empty;
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

            using (var buffer = new IndexingBuffer(
                bufferSizes.indexSize,
                bufferSizes.contentSize,
                _initialized.IndexFile.PushDocumentsAsync))
            {
                var batchCount = 0;
                long lastTimeStamp = 0;

                await foreach (var batch in _initialized.LogFile.ReadDocumentsFromLogFileAsync(
                    _initialized.IndexFile.LastDocumentTimeStamp,
                    _indexConstants.MaxLogBufferSize))
                {
                    lastTimeStamp = batch.BatchTimeStamp;
                    ++batchCount;
                    foreach (var item in batch.Items)
                    {
                        var (metaData, content) = SplitDocument(item, _initialized.LogFile.PartitionParts);

                        await buffer.WriteAsync(metaData, content);
                    }
                }
                await buffer.FlushAsync();
                _initialized.IndexFile.UpdateLastDocumentTimeStamp(lastTimeStamp);

                _logger.Display($"Indexed {buffer.ItemCount} documents in {batchCount} batches");
                _logger
                    .AddContext("documentCount", buffer.ItemCount)
                    .AddContext("batchCount", batchCount)
                    .WriteEvent("Index-Collection-Documents-End");
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

        private (int indexSize, int contentSize) GetStoredProcedureBufferSizes()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var logSize = _initialized.LogFile.GetSprocLogSize(
                _initialized.IndexFile.LastDocumentTimeStamp);
            var indexSize = (int)Math.Min(
                logSize / (_indexConstants.MaxContentBufferSize / _indexConstants.MaxIndexBufferSize),
                (long)_indexConstants.MaxIndexBufferSize);
            var contentSize = (int)Math.Min(
                logSize,
                (long)_indexConstants.MaxContentBufferSize);

            return (indexSize, contentSize);
        }

        private static (DocumentMetaData metaData, byte[] content) SplitDocument(
            JsonElement doc,
            IImmutableList<string> partitionParts)
        {
            var id = doc.GetProperty(Constants.ID_FIELD).GetString();
            var partitionKey = GetPartitionKey(doc, partitionParts);
            var timeStamp = doc.GetProperty(Constants.TIMESTAMP_FIELD).GetInt64();
            var content = JsonSerializer.SerializeToUtf8Bytes(CleanDocument(doc));
            var metaData = new DocumentMetaData(
                id,
                partitionKey,
                timeStamp,
                content.Length);

            return (metaData, content);
        }

        private static object? GetPartitionKey(JsonElement doc, IImmutableList<string> partitionParts)
        {
            JsonElement current = doc;

            for (int i = 0; i != partitionParts.Count; ++i)
            {
                var part = partitionParts[i];
                JsonElement output;
                var hasProperty = doc.TryGetProperty(part, out output);

                if (!hasProperty || i == partitionParts.Count - 1)
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

        private static IImmutableDictionary<string, JsonElement> CleanDocument(JsonElement doc)
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