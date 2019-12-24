using Cosbak.Controllers.LogBackup;
using Cosbak.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    internal class ReadonlyLogFile
    {
        #region Inner Types
        private class Initialized
        {
            public Initialized(
                DateTimeOffset? snapshotTime,
                LogFat fat,
                IImmutableList<BlockItem> blocks)
            {
                SnapshotTime = snapshotTime;
                Fat = fat;
                Blocks = blocks;
            }

            public DateTimeOffset? SnapshotTime { get; }

            public LogFat Fat { get; }

            public IImmutableList<BlockItem> Blocks { get; }
        }
        #endregion

        private readonly IStorageFacade _storageFacade;
        private readonly string _blobName;
        private readonly ILogger _logger;
        private Initialized? _initialized;

        public ReadonlyLogFile(
            IStorageFacade storageFacade,
            string accountName,
            string databaseName,
            string collectionName,
            ILogger logger)
        {
            _storageFacade = storageFacade.ChangeFolder(
                $"{Constants.BACKUPS_FOLDER}/{accountName}/{databaseName}");
            _blobName = $"{collectionName}.{Constants.LOG_EXTENSION}";
            _logger = logger;
        }

        public long LastTimeStamp
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.Fat.LastTimeStamp;
            }
        }

        public long LastCheckpointTimeStamp
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.Fat.LastCheckpointTimeStamp;
            }
        }

        public async Task InitializeAsync()
        {
            if (_initialized != null)
            {
                throw new InvalidOperationException("InitializeAsync has already been called");
            }

            var snapshotTime = await _storageFacade.SnapshotAsync(_blobName);
            var blocks = await _storageFacade.GetBlocksAsync(_blobName, snapshotTime);
            var logFat = blocks.Count == 0
                ? new LogFat()
                : await LoadFatAsync((int)blocks[0].Length, snapshotTime);

            _initialized = new Initialized(snapshotTime, logFat, blocks);
        }

        public async Task DisposeAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            await _storageFacade.ClearSnapshotsAsync(_blobName);
        }

        public long GetDocumentLogSize(long afterTimeStamp)
        {
            var blocks = GetDocumentBlocks(afterTimeStamp);
            var sizes = from b in blocks
                        select b.block.Size;
            var totalSize = sizes.Sum();

            return totalSize;
        }

        public async IAsyncEnumerable<(JsonElement doc, long batchTimeStamp)> ReadDocumentsAsync(
            long afterTimeStamp,
            int maxBufferSize)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var blocks = GetDocumentBlocks(afterTimeStamp);
            var remainingBlocks = blocks.ToImmutableList();
            long batchTimeStamp = 0;

            while (remainingBlocks.Any())
            {
                var pageBatch = GetPageFromBlocks(remainingBlocks, maxBufferSize).ToImmutableArray();
                var (start, end) = GetInterval(
                    pageBatch.Select(b => b.block.Id),
                    _initialized.Blocks);
                var bufferIndex = 0;

                using (var buffer = BufferPool.Rent((int)(end - start)))
                {
                    await _storageFacade.DownloadRangeAsync(
                        _blobName,
                        buffer.Buffer,
                        start,
                        end - start,
                        _initialized.SnapshotTime);
                    remainingBlocks = remainingBlocks.RemoveRange(pageBatch);

                    foreach (var page in pageBatch)
                    {
                        var sequence = new ReadOnlySequence<byte>(buffer.Buffer)
                            .Slice(bufferIndex, (int)page.block.Size);

                        foreach (var item in ReadItemFromBuffer(sequence))
                        {
                            yield return (item, batchTimeStamp);
                        }
                        //  Completed batch:  commit timestamp
                        batchTimeStamp = page.TimeStamp;
                        bufferIndex += (int)page.block.Size;
                    }
                }
            }
        }

        private IEnumerable<(Block block, long TimeStamp)> GetDocumentBlocks(long afterTimeStamp)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var checkPoints = from p in _initialized.Fat.CheckPoints
                              where p.TimeStamp > afterTimeStamp
                              select p;
            var batches = from b in checkPoints
                          .SelectMany(p => p.DocumentBatches)
                          .Concat(_initialized.Fat.InProgressDocumentBatches)
                          where b.TimeStamp > afterTimeStamp
                          select b;
            var blocks = batches.SelectMany(b => from block in b.Blocks
                                                 select (block, b.TimeStamp));

            return blocks;
        }

        private static IEnumerable<JsonElement> ReadItemFromBuffer(
            ReadOnlySequence<byte> sequence)
        {
            var initialReader = new Utf8JsonReader(sequence);

            //  To start object
            initialReader.Read();
            //  To property
            initialReader.Read();
            while (GetValue(initialReader) != "Documents")
            {
                //  To property value
                initialReader.Read();
                //  To next item
                initialReader.Read();
            }
            //  To Property value
            initialReader.Read();
            if (initialReader.TokenType != JsonTokenType.StartArray)
            {
                throw new InvalidOperationException(
                    $"Should be a start array in the JSON instead of '{initialReader.TokenType}'");
            }
            //  To first element in the array
            initialReader.Read();

            //  Work with index since Utf8JsonReader can't be used inside broken loops
            var index = initialReader.TokenStartIndex;
            do
            {
                var result = DeserializeElement(sequence.Slice(index));

                yield return result.element;
                index += result.offset;

                if (!result.shouldContinueLoop)
                {
                    break;
                }
            }
            while (true);
        }

        private static (
            JsonElement element,
            bool shouldContinueLoop,
            long offset) DeserializeElement(ReadOnlySequence<byte> sequence)
        {
            //  This whole intricate routine exists because Utf8JsonReader can't exist
            //  inside a broken loop (i.e. a loop with yields)
            var reader = new Utf8JsonReader(sequence);
            var element = JsonSerializer.Deserialize<JsonElement>(ref reader);
            var shouldContinueLoop = reader.TokenType == JsonTokenType.StartObject;

            return (element, shouldContinueLoop, reader.TokenStartIndex);
        }

        private static string GetValue(Utf8JsonReader reader)
        {
            var value = ASCIIEncoding.UTF8.GetString(reader.ValueSpan);

            return value;
        }

        private IEnumerable<(Block block, long TimeStamp)> GetPageFromBlocks(
            IEnumerable<(Block block, long TimeStamp)> remainingBlocks,
            int maxSize)
        {
            int count = 0;

            foreach (var i in remainingBlocks)
            {
                if (i.block.Size > maxSize)
                {
                    if (count == 0)
                    {
                        throw new NotSupportedException(
                            $"Block too big to be indexed:  ({i.block.Id}, {i.block.Size})");
                    }

                    return remainingBlocks.Take(count);
                }
                else
                {
                    ++count;
                    maxSize -= (int)i.block.Size;
                }
            }

            return remainingBlocks;
        }

        private (long start, long end) GetInterval(
            IEnumerable<string> blockIds,
            IImmutableList<BlockItem> blocks)
        {
            long start = 0;
            long index = 0;
            var isStarted = false;

            foreach (var block in blocks)
            {
                if (block.Id == blockIds.First())
                {
                    if (!isStarted)
                    {
                        isStarted = true;
                        start = index;
                    }
                    else
                    {
                    }
                    blockIds = blockIds.Skip(1);
                    if (!blockIds.Any())
                    {
                        return (start, index + block.Length);
                    }
                }
                else
                {
                    if (!isStarted)
                    {
                    }
                    else
                    {
                        throw new NotSupportedException("Blocks aren't contiguous");
                    }
                }
                index += block.Length;
            }

            throw new NotSupportedException("Remaining blocks that aren't in the blob");
        }

        private async Task<LogFat> LoadFatAsync(int length, DateTimeOffset? snapshotTime)
        {
            var buffer = new byte[length];

            await _storageFacade.DownloadRangeAsync(
                _blobName,
                buffer,
                snapshotTime: snapshotTime);

            return JsonSerializer.Deserialize<LogFat>(buffer);
        }
    }
}