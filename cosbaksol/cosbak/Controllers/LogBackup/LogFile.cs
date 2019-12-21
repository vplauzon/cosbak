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
            public Initialized(BlobLease lease, LogFat logFat)
            {
                Lease = lease;
                LogFat = logFat;
            }

            public BlobLease Lease { get; }

            public LogFat LogFat { get; }
        }
        #endregion

        private readonly IStorageFacade _storageFacade;
        private readonly string _blobName;
        private readonly ILogger _logger;
        private Initialized? _initialized;
        private bool _isDirty = false;

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

        public long LastTimeStamp
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.LogFat.LastTimeStamp;
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

                return _initialized.LogFat.LastCheckpointTimeStamp;
            }
        }

        public int TotalDocumentBlockCount
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.LogFat.GetDocumentsBlocks().Count();
            }
        }

        public long TotalDocumentSize
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.LogFat.GetDocumentsBlocks().Sum(b => b.Size);
            }
        }

        public int TotalCheckPointBlockCount
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.LogFat.GetCheckpointBlocks().Count();
            }
        }

        public long TotalCheckPointSize
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.LogFat.GetCheckpointBlocks().Sum(b => b.Size);
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

            var leaseController = new LeaseController(_logger, _storageFacade);
            var lease = await leaseController.AcquireLeaseAsync(_blobName, TimeSpan.FromMinutes(1));

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

                _initialized = new Initialized(lease, logFat);
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
                var fatBuffer = JsonSerializer.SerializeToUtf8Bytes(_initialized.LogFat);
                var fatBlock = await WriteBlockAsync(fatBuffer, fatBuffer.Length);
                var blocks = _initialized.LogFat.GetCheckpointBlocks()
                    .Concat(_initialized.LogFat.GetDocumentsBlocks())
                    .Prepend(fatBlock);

                _storageFacade.WriteAsync(_blobName, blocks.Select(b => b.Id), _initialized.Lease);
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

        public async Task<Block> WriteBlockAsync(byte[] buffer, int length)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var blockId = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

            await _storageFacade.WriteBlockAsync(
                _blobName, blockId, buffer, length, _initialized.Lease);

            return new Block
            {
                Id = blockId,
                Size = length
            };
        }

        public void AddDocumentBatch(long timeStamp, IImmutableList<Block> blocks)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            if (blocks.Any())
            {
                _initialized.LogFat.AddDocumentBatch(timeStamp, blocks);
                _isDirty = true;
            }
        }

        public void CreateCheckpoint(
            long timeStamp,
            IImmutableList<Block>? idsBlocks,
            IImmutableList<Block>? sprocsBlocks,
            IImmutableList<Block>? functionsBlocks,
            IImmutableList<Block>? triggersBlocks)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }
            _initialized.LogFat.CreateCheckPoint(
                timeStamp,
                idsBlocks,
                sprocsBlocks,
                functionsBlocks,
                triggersBlocks);
            _isDirty = true;
        }

        private async Task<LogFat> LoadLogFatAsync(int length)
        {
            var buffer = new byte[length];

            await _storageFacade.DownloadRangeAsync(_blobName, buffer);

            return JsonSerializer.Deserialize<LogFat>(buffer);
        }
    }
}