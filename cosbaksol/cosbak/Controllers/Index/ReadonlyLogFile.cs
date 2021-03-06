﻿using Cosbak.Controllers.LogBackup;
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
                PartitionParts = Fat.PartitionPath
                    .Split('/')
                    .Skip(1)
                    .ToImmutableArray();
                Blocks = blocks;
            }

            public DateTimeOffset? SnapshotTime { get; }

            public LogFat Fat { get; }

            public IImmutableList<string> PartitionParts { get; }

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
                $"{Constants.BACKUPS_FOLDER}/{accountName}");
            _blobName = $"{databaseName}.{collectionName}.{Constants.LOG_EXTENSION}";
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

        public IImmutableList<string> PartitionParts
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.PartitionParts;
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

        public long GetSprocLogSize(long afterTimeStamp)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var checkPoints = from p in _initialized.Fat.CheckPoints
                              where p.TimeStamp > afterTimeStamp
                              select p;
            var blocks = checkPoints
                .SelectMany(p => p.SprocsBlocks);
            var sizes = from b in blocks
                        select b.Size;
            var totalSize = sizes.Sum();

            return totalSize;
        }

        public IAsyncEnumerable<LogDocumentBatch> ReadDocumentsFromLogFileAsync(
            long afterTimeStamp,
            int maxBufferSize)
        {
            return ReadItemsFromLogFileAsync(
                maxBufferSize,
                GetDocumentBlocks(afterTimeStamp),
                (long batchTimeStamp, ReadOnlySequence<byte> sequence) =>
                new LogDocumentBatch(batchTimeStamp, sequence));
        }

        public IAsyncEnumerable<LogSprocBatch> ReadStoredProceduresAsync(
            long afterTimeStamp,
            int maxBufferSize)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var checkPoints = from p in _initialized.Fat.CheckPoints
                              where p.TimeStamp > afterTimeStamp
                              select p;
            var blocks = checkPoints
                .SelectMany(p => p.SprocsBlocks.Select(b => (b, p.TimeStamp)));

            return ReadItemsFromLogFileAsync(
                maxBufferSize,
                blocks,
                (long batchTimeStamp, ReadOnlySequence<byte> sequence) =>
                new LogSprocBatch(batchTimeStamp, sequence));
        }

        public async IAsyncEnumerable<T> ReadItemsFromLogFileAsync<T>(
            int maxBufferSize,
            IEnumerable<(Block block, long TimeStamp)> blocks,
            Func<long, ReadOnlySequence<byte>, T> itemDeserializer)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var remainingBlocks = blocks.ToImmutableList();

            while (remainingBlocks.Any())
            {
                var pageBatch =
                    GetPageFromBlocks(remainingBlocks, maxBufferSize).ToImmutableArray();
                var (start, end) = Block.GetInterval(
                    pageBatch.Select(b => b.block),
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
                        var batch = itemDeserializer(page.TimeStamp, sequence);

                        yield return batch;
                        bufferIndex += (int)page.block.Size;
                    }
                }
            }
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