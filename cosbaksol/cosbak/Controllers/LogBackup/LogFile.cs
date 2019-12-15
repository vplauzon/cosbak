using Cosbak.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cosbak.Controllers.LogBackup
{
    internal class LogFile
    {
        #region Inner Types
        private class Initialized
        {
            public Initialized(BlobLease lease, IImmutableList<BlockItem> blocks, LogFat logFat)
            {
                Lease = lease;
                Blocks = blocks;
                LogFat = logFat;
            }

            public BlobLease Lease { get; }

            public IImmutableList<BlockItem> Blocks { get; }

            public LogFat LogFat { get; }
        }
        #endregion

        private readonly IStorageFacade _storageFacade;
        private readonly string _blobName;
        private readonly ILogger _logger;
        private Initialized? _initialized;

        public LogFile(
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

        public long LastUpdateTime
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.LogFat.LastUpdateTime;
            }
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

            var lease = await _storageFacade.GetLeaseAsync(_blobName);

            if (lease == null)
            {
                throw new CosbakException($"Can't lease '{_blobName}' blob");
            }
            else
            {
                var blocks = await _storageFacade.GetBlocksAsync(_blobName);
                var logFat = blocks.Count == 0
                    ? new LogFat()
                    : await LoadLogFatAsync((int)blocks[0].Length);

                _initialized = new Initialized(lease, blocks, logFat);
            }
        }

        public async Task PersistAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var fatBuffer = JsonSerializer.SerializeToUtf8Bytes(_initialized.LogFat);
            var fatBlockName = await WriteBlockAsync(fatBuffer, fatBuffer.Length);
            var blockNames = _initialized.LogFat.GetAllBlockNames();

            blockNames = blockNames.Insert(0, fatBlockName);
            _storageFacade.WriteAsync(_blobName, blockNames, _initialized.Lease);
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

            var blockName = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

            await _storageFacade.WriteBlockAsync(
                _blobName, blockName, buffer, length, _initialized.Lease);

            return blockName;
        }

        public void AddDocumentBatch(long timeStamp, ImmutableList<string> blockNames)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            _initialized.LogFat.AddDocumentBatch(timeStamp, blockNames);
        }

        private async Task<LogFat> LoadLogFatAsync(int length)
        {
            var buffer = new byte[length];

            await _storageFacade.DownloadRangeAsync(_blobName, buffer);

            return JsonSerializer.Deserialize<LogFat>(buffer);
        }
    }
}