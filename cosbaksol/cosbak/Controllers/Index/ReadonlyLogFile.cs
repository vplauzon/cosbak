using Cosbak.Controllers.LogBackup;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
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
            public Initialized(DateTimeOffset? snapshotTime, LogFat fat)
            {
                SnapshotTime = snapshotTime;
                Fat = fat;
            }

            public DateTimeOffset? SnapshotTime { get; }

            public LogFat Fat { get; }
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

            _initialized = new Initialized(snapshotTime, logFat);
        }

        public async Task DisposeAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            await _storageFacade.ClearSnapshotsAsync(_blobName);
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