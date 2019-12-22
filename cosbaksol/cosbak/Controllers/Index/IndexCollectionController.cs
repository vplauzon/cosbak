using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
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
                public JsonElement[] Documents { get; set; } = new JsonElement[0];
            }
            #endregion

            private readonly IndexFile _indexFile;
            private readonly ReadonlyLogFile _logFile;
            private readonly IImmutableList<string> _partitionParts;
            private readonly IndexConstants _indexConstants;

            public IndexIterationController(
                IndexFile indexFile,
                ReadonlyLogFile logFile,
                string partitionPath,
                IndexConstants indexConstants)
            {
                _indexFile = indexFile;
                _logFile = logFile;
                _partitionParts = partitionPath.Split('/').Skip(1).ToImmutableArray();
                _indexConstants = indexConstants;
            }

            public async Task IndexAsync(bool needCheckpointPurge)
            {
                await IndexDocumentsAsync();
            }

            private async Task IndexDocumentsAsync()
            {
                var indexBuffer = new byte[_indexConstants.MaxIndexBufferSize];
                var contentBuffer = new byte[_indexConstants.MaxContentBufferSize];
                var enumerable = _logFile.LoadDocumentBufferAsync(
                    _indexFile.LastDocumentTimeStamp,
                    _indexConstants.MaxLogBufferSize);

                using (var indexStream = new MemoryStream(indexBuffer))
                using (var contentStream = new MemoryStream(contentBuffer))
                using (var contentWriter = new StreamWriter(contentStream))
                {
                    await foreach (var logBuffer in enumerable)
                    {
                        foreach (var buffer in logBuffer.Buffers)
                        {
                            var documents = GetDocuments(buffer.Span);

                            foreach (var doc in documents)
                            {
                                var (metaData, content) = SplitDocument(doc);

                                metaData.Write(indexStream);
                                contentWriter.Write(content);
                                contentWriter.Flush();
                            }
                            //await _indexFile.PersistAsync();
                        }
                    }
                }
            }

            private IImmutableList<JsonElement> GetDocuments(ReadOnlySpan<byte> span)
            {
                var logged = JsonSerializer.Deserialize<LoggedDocuments>(span);

                if (logged == null || logged.Documents == null || !logged.Documents.Any())
                {
                    throw new NotSupportedException("Logged block non-compliant");
                }

                return logged.Documents.ToImmutableList();
            }

            private (DocumentMetaData metaData, string content) SplitDocument(JsonElement doc)
            {
                var id = doc.GetProperty(Constants.ID_FIELD).GetString();
                var partitionKey = GetPartitionKey(doc);
                var timeStamp = doc.GetProperty(Constants.TIMESTAMP_FIELD).GetInt64();
                var content = JsonSerializer.Serialize(CleanDocument(doc));
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
                        _collection.PartitionPath,
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