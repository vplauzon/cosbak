using Cosbak.Controllers.LogBackup;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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

        public struct LogBuffers
        {
            public IImmutableList<ReadOnlyMemory<byte>> Buffers { get; set; }

            public long LastTimeStamp { get; set; }
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


        public async IAsyncEnumerable<LogBuffers> LoadDocumentBufferAsync(
            long afterTimeStamp,
            int maxSize)
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
            var remainingBlocks = blocks.ToImmutableList();
            var buffer = new byte[maxSize];

            while (remainingBlocks.Any())
            {
                var pageBatch = GetPageFromBlocks(remainingBlocks, maxSize).ToImmutableArray();
                var (start, end) = GetInterval(
                    pageBatch.Select(b => b.block.Id),
                    _initialized.Blocks);

                await _storageFacade.DownloadRangeAsync(
                    _blobName,
                    buffer,
                    start,
                    end - start,
                    _initialized.SnapshotTime);
                remainingBlocks = remainingBlocks.RemoveRange(pageBatch);

                yield return new LogBuffers
                {
                    Buffers = FoldBuffers(
                        pageBatch,
                        new ReadOnlyMemory<byte>(buffer, 0, (int)(end - start))),
                    LastTimeStamp = remainingBlocks.Any()
                    //  Commit only for the most conservative timestamp
                    ? pageBatch.First().TimeStamp
                    : pageBatch.Last().TimeStamp
                };
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

        private IImmutableList<ReadOnlyMemory<byte>> FoldBuffers(
            IEnumerable<(Block block, long TimeStamp)> pageBatch,
            ReadOnlyMemory<byte> buffer)
        {
            var pieces = ImmutableList<ReadOnlyMemory<byte>>.Empty;
            int index = 0;

            foreach (var page in pageBatch)
            {
                var span = buffer.Slice(index, (int)page.block.Size);

                pieces = pieces.Add(span);
                index += (int)page.block.Size;
            }

            return pieces;
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