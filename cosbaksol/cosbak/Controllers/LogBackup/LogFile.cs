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
            public Initialized(BlobLease lease, LogFat fat)
            {
                Lease = lease;
                Fat = fat;
            }

            public BlobLease Lease { get; }

            public LogFat Fat { get; }
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

        public int TotalDocumentBlockCount
        {
            get
            {
                if (_initialized == null)
                {
                    throw new InvalidOperationException("InitializeAsync hasn't been called");
                }

                return _initialized.Fat.GetInProgressDocumentsBlocks().Count();
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

                return _initialized.Fat.GetInProgressDocumentsBlocks().Sum(b => b.Size);
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

                return _initialized.Fat.GetCheckpointBlocks().Count();
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

                return _initialized.Fat.GetCheckpointBlocks().Sum(b => b.Size);
            }
        }

        public TimeSpan GetOldestCheckpointAge(long currentTimeStamp)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            return _initialized.Fat.CheckPoints.Any()
                ? TimeSpan.FromSeconds(
                    currentTimeStamp - _initialized.Fat.CheckPoints[0].TimeStamp)
                : TimeSpan.Zero;
        }

        public async Task InitializeAsync(string partitionPath)
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
                    ? new LogFat { PartitionPath = partitionPath }
                    : await LoadFatAsync((int)blocks[0].Length, partitionPath);

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
                var fatBuffer = JsonSerializer.SerializeToUtf8Bytes(_initialized.Fat);
                var fatBlock = await WriteBlockAsync(fatBuffer, fatBuffer.Length);
                var blocks = _initialized.Fat.GetAllBlocks()
                    .Prepend(fatBlock);

                await _storageFacade.WriteAsync(
                    _blobName,
                    blocks.Select(b => b.Id),
                    _initialized.Lease);
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

            _logger.Display($"Write logs {length} bytes...");
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
                _initialized.Fat.AddDocumentBatch(timeStamp, blocks);
                _isDirty = true;
            }
        }

        public void Purge(bool isDocumentOnlyPurge, long lastTimestamp)
        {
            if (!isDocumentOnlyPurge)
            {
            }

            PurgeDocuments(lastTimestamp);
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
            _initialized.Fat.CreateCheckPoint(
                timeStamp,
                idsBlocks,
                sprocsBlocks,
                functionsBlocks,
                triggersBlocks);
            _isDirty = true;
        }

        private async Task<LogFat> LoadFatAsync(int length, string partitionPath)
        {
            var buffer = new byte[length];

            await _storageFacade.DownloadRangeAsync(_blobName, buffer);

            var logFat = JsonSerializer.Deserialize<LogFat>(buffer);

            if (logFat.PartitionPath != partitionPath)
            {
                throw new CosbakException(
                    $"Partition path changed from {logFat.PartitionPath} to {partitionPath}");
            }

            return logFat;
        }

        private void PurgeDocuments(long lastTimestamp)
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            _initialized.Fat.PurgeDocuments(lastTimestamp);
            _isDirty = true;
        }
    }
}