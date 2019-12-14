using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading.Tasks;
using Cosbak.Storage;
using Newtonsoft.Json;

namespace Cosbak.Controllers.Backup
{
    public class StorageCollectionController : IStorageCollectionController
    {
        private readonly IStorageFacade _rootStorage;
        private readonly ILogger _logger;
        private BlobLease? _lease;
        private MasterBackupData? _master;
        private bool _isMasterDirty = false;
        private int _contentFolderId;
        private IStorageFacade? _contentStorage;

        public StorageCollectionController(IStorageFacade storageFacade, ILogger logger)
        {
            _rootStorage = storageFacade;
            _logger = logger;
        }

        public async Task InitializeAsync()
        {
            if (!await _rootStorage.DoesExistAsync(Constants.BACKUP_MASTER))
            {   //  Create empty master
                await _rootStorage.UploadBlockBlobAsync(Constants.BACKUP_MASTER, string.Empty);
            }

            _lease = await _rootStorage.GetLeaseAsync(Constants.BACKUP_MASTER);

            if (_lease == null)
            {
                throw new CosbakException($"Can't lease '{Constants.BACKUP_MASTER}' blob");
            }
            else
            {
                var masterContent = await _rootStorage.GetContentAsync(Constants.BACKUP_MASTER);
                var serializer = new JsonSerializer();

                using (var stringReader = new StringReader(masterContent))
                using (var reader = new JsonTextReader(stringReader))
                {
                    _master = serializer.Deserialize<MasterBackupData>(reader)
                        ?? new MasterBackupData();
                }

                var maxFolderId = _master.Batches.Any()
                    ? _master.Batches.Select(f => f.FolderId).Max()
                    : 0;

                _contentFolderId = maxFolderId + 1;
                _contentStorage = _rootStorage.ChangeFolder(_contentFolderId.ToString());

                await CleanFolderAsync();
            }
        }

        long? IStorageCollectionController.LastContentTimeStamp =>
            (_master ?? throw new NotSupportedException("Null master")).LastContentTimeStamp;

        void IStorageCollectionController.UpdateContent(long lastContentTimeStamp)
        {
            _isMasterDirty = true;

            (_master ?? throw new NotSupportedException("Null master")).LastContentTimeStamp = lastContentTimeStamp;
            _master.Batches.Add(new BackupBatchData
            {
                FolderId = _contentFolderId,
                TimeStamp = lastContentTimeStamp
            });
        }

        IStoragePartitionController IStorageCollectionController.GetPartition(string id)
        {
            return new StoragePartitionController(
                id,
                (_contentStorage ?? throw new NotSupportedException("Content storage")),
                _logger);
        }

        async Task IStorageCollectionController.ReleaseAsync()
        {
            if (_isMasterDirty)
            {
                await UpdateMasterAsync();
            }

            await (_lease ?? throw new NotSupportedException("Lease")).ReleaseLeaseAsync();
        }

        private async Task CleanFolderAsync()
        {
            var contentFolders = (from cf in (_master ?? throw new NotSupportedException("Null master")).Batches
                                  select cf.FolderId.ToString()).ToArray();
            Func<string, bool> keepFilter = (path) =>
            //  Keep master
            path == Constants.BACKUP_MASTER
            //  Keep all blobs under content folders
            || contentFolders.Any(f => path.StartsWith(f + '/'));
            Func<string, bool> toDeleteFilter = (path) => !keepFilter(path);
            var blobPathList = await _rootStorage.ListBlobsAsync(toDeleteFilter);
            var tasks = from path in blobPathList
                        select _rootStorage.DeleteBlobAsync(path);

            await Task.WhenAll(tasks);
        }

        private async Task UpdateMasterAsync()
        {
            var serializer = new JsonSerializer();

            using (var stringWriter = new StringWriter())
            using (var writer = new JsonTextWriter(stringWriter))
            {
                serializer.Serialize(writer, _master);

                var masterContent = stringWriter.ToString();

                await _rootStorage.UploadBlockBlobAsync(
                    Constants.BACKUP_MASTER,
                    masterContent,
                    (_lease ?? throw new NotSupportedException("Lease")).LeaseId);
            }
        }
    }
}