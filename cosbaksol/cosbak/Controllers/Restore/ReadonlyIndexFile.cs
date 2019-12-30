using Cosbak.Controllers.Index;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Restore
{
    internal class ReadonlyIndexFile
    {
        #region Inner Types
        private class Initialized
        {
            public Initialized(
                DateTimeOffset? snapshotTime,
                IndexFat fat,
                IImmutableList<BlockItem> blobBlocks)
            {
                SnapshotTime = snapshotTime;
                Fat = fat;
                BlobBlocks = blobBlocks;
            }

            public DateTimeOffset? SnapshotTime { get; }

            public IndexFat Fat { get; }

            public IImmutableList<BlockItem> BlobBlocks { get; }
        }
        #endregion

        private readonly IStorageFacade _storageFacade;
        private readonly string _blobName;
        private readonly ILogger _logger;
        private Initialized? _initialized;

        public ReadonlyIndexFile(
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

        public async Task InitializeAsync()
        {
            if (_initialized != null)
            {
                throw new InvalidOperationException("InitializeAsync has already been called");
            }

            var snapshotTime = await _storageFacade.SnapshotAsync(_blobName);
            var blocks = await _storageFacade.GetBlocksAsync(_blobName, snapshotTime);

            if (blocks.Count == 0)
            {
                throw new CosbakException("Index file is empty");
            }
            else
            {
                var indexFat = blocks.Count == 0
                    ? new IndexFat()
                    : await LoadFatAsync((int)blocks[0].Length, snapshotTime);

                _initialized = new Initialized(snapshotTime, indexFat, blocks);
            }
        }

        public async IAsyncEnumerable<IEnumerable<Stream>> ReadLatestDocumentsAsync(long upToTimeStamp)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }
            using (var indexBuffer =
                await LoadBlocksAsync(_initialized.Fat.DocumentPartition.IndexBlocks))
            using (var contentBuffer =
                await LoadBlocksAsync(_initialized.Fat.DocumentPartition.ContentBlocks))
            {
                var indexed = new SortedIndexedDocumentEnumerable(
                    indexBuffer.Buffer,
                    _initialized.Fat.DocumentPartition.IndexBlocks.Sum(b => (int)b.Size),
                    contentBuffer.Buffer);
                var latestDocuments = from i in indexed.GetLatestItems(upToTimeStamp)
                                      select i.content;

                yield return latestDocuments;
            }
        }

        public async Task DisposeAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            await _storageFacade.ClearSnapshotsAsync(_blobName);
        }

        private async Task<BufferPool> LoadBlocksAsync(IImmutableList<Block> blocks)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var (start, end) = Block.GetInterval(blocks, _initialized.BlobBlocks);
            var buffer = BufferPool.Rent((int)(end - start));

            await _storageFacade.DownloadRangeAsync(
                _blobName,
                buffer.Buffer,
                start,
                end - start,
                _initialized.SnapshotTime);

            return buffer;
        }

        private async Task<IndexFat> LoadFatAsync(int length, DateTimeOffset? snapshotTime)
        {
            var buffer = new byte[length];

            await _storageFacade.DownloadRangeAsync(
                _blobName,
                buffer,
                snapshotTime: snapshotTime);

            return JsonSerializer.Deserialize<IndexFat>(buffer);
        }
    }
}