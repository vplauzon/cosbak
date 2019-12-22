using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    internal class IndexFile
    {
        #region Inner Types
        private class Initialized
        {
            public Initialized(BlobLease lease, IndexFat fat)
            {
                Lease = lease;
                Fat = fat;
            }

            public BlobLease Lease { get; }

            public IndexFat Fat { get; }
        }
        #endregion

        private readonly IStorageFacade _storageFacade;
        private readonly string _blobName;
        private readonly ILogger _logger;
        private Initialized? _initialized;
        private bool _isDirty = false;

        public IndexFile(
            IStorageFacade storageFacade,
            string accountName,
            string databaseName,
            string collectionName,
            ILogger logger)
        {
            _storageFacade = storageFacade.ChangeFolder(
                $"{Constants.BACKUPS_FOLDER}/{accountName}/{databaseName}");
            _blobName = $"{collectionName}.{Constants.INDEX_EXTENSION}";
            _logger = logger;
        }

        public async Task InitializeAsync()
        {
            if (_initialized != null)
            {
                throw new InvalidOperationException("InitializeAsync has already been called");
            }

            var doesExist = await _storageFacade.DoesExistAsync(_blobName);

            if (!doesExist)
            {
                await _storageFacade.CreateEmptyBlockBlobAsync(_blobName);
            }

            var leaseController = new LeaseController(_logger, _storageFacade);
            var lease = await leaseController.AcquireLeaseAsync(
                _blobName,
                TimeSpan.FromMinutes(1));

            if (lease == null)
            {
                throw new CosbakException($"Can't lease '{_blobName}' blob");
            }
            else
            {
                var blocks = await _storageFacade.GetBlocksAsync(_blobName);
                var indexFat = blocks.Count == 0
                    ? new IndexFat()
                    : await LoadFatAsync((int)blocks[0].Length);

                _initialized = new Initialized(lease, indexFat);
            }
        }

        public async Task PersistAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            if (_isDirty)
            {
                var fatBuffer = JsonSerializer.SerializeToUtf8Bytes(_initialized.Fat);
                var fatBlockId = await WriteBlockAsync(fatBuffer, fatBuffer.Length);
                var blockIds = new[] { fatBlockId };

                await _storageFacade.WriteAsync(_blobName, blockIds, _initialized.Lease);
                _isDirty = false;
            }
        }

        public async Task DisposeAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }
            await _initialized.Lease.ReleaseLeaseAsync();
        }

        public async Task<string> WriteBlockAsync(byte[] buffer, int length)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var blockId = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

            await _storageFacade.WriteBlockAsync(
                _blobName, blockId, buffer, length, _initialized.Lease);

            return blockId;
        }

        private async Task<IndexFat> LoadFatAsync(int length)
        {
            var buffer = new byte[length];

            await _storageFacade.DownloadRangeAsync(_blobName, buffer);

            return JsonSerializer.Deserialize<IndexFat>(buffer);
        }
    }
}