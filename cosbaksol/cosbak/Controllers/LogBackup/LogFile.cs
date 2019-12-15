using Cosbak.Storage;
using System;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Controllers.LogBackup
{
    internal class LogFile
    {
        #region Inner Types
        private class Initialized
        {
            public Initialized(BlobLease lease, IImmutableList<BlockItem> blocks)
            {
                Lease = lease;
                Blocks = blocks;
            }

            public BlobLease Lease { get; }
            
            public IImmutableList<BlockItem> Blocks { get; }
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
            _storageFacade = storageFacade.ChangeFolder($"backups/{accountName}/{databaseName}");
            _blobName = $"{collectionName}.{Constants.LOG_EXTENSION}";
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

            var lease = await _storageFacade.GetLeaseAsync(_blobName);

            if (lease == null)
            {
                throw new CosbakException($"Can't lease '{_blobName}' blob");
            }
            else
            {
                var blocks = await _storageFacade.GetBlocksAsync(_blobName);

                _initialized = new Initialized(lease, blocks);
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
    }
}