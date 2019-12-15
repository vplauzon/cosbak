using Cosbak.Storage;
using System;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Log
{
    internal class LogFile
    {
        #region Inner Types
        private class Initialized
        {
            public Initialized(BlobLease lease)
            {
                Lease = lease;
            }

            public BlobLease Lease { get; }
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
            if (_initialized != null )
            {
                throw new InvalidOperationException("InitializeAsync has already been called");
            }

            var lease = await _storageFacade.GetLeaseAsync(_blobName);

            if (lease == null)
            {
                throw new CosbakException($"Can't lease '{_blobName}' blob");
            }
            else
            {
                _initialized = new Initialized(lease);
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