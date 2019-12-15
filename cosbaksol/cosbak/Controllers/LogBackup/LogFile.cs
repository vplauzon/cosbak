using Cosbak.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;
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

        public int LastUpdateTime
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
                    : throw new NotImplementedException();

                _initialized = new Initialized(lease, blocks, logFat);
            }
        }

        public async Task PersistAsync()
        {
            await Task.FromResult(42);
            throw new NotImplementedException();
        }

        public async Task DisposeAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }
            await _initialized.Lease.ReleaseLeaseAsync();
        }
    }
}