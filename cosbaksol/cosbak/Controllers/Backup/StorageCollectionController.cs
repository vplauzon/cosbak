using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Cosbak.Storage;
using Newtonsoft.Json;

namespace Cosbak.Controllers.Backup
{
    public class StorageCollectionController : IStorageCollectionController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly ILogger _logger;
        private BlobLease _lease;
        private MasterBackupData _master;
        private bool _isMasterDirty = false;

        public StorageCollectionController(IStorageFacade storageFacade, ILogger logger)
        {
            _storageFacade = storageFacade;
            _logger = logger;
        }

        public async Task InitializeAsync()
        {
            if (!await _storageFacade.DoesExistAsync(Constants.BACKUP_MASTER))
            {   //  Create empty master
                await _storageFacade.UploadBlockBlobAsync(Constants.BACKUP_MASTER, string.Empty);
            }

            _lease = await _storageFacade.GetLeaseAsync(Constants.BACKUP_MASTER);

            if (_lease == null)
            {
                throw new CosbakException($"Can't lease '{Constants.BACKUP_MASTER}' blob");
            }
            else
            {
                var masterContent = await _storageFacade.GetContentAsync(Constants.BACKUP_MASTER);
                var serializer = new JsonSerializer();

                using (var stringReader = new StringReader(masterContent))
                using (var reader = new JsonTextReader(stringReader))
                {
                    _master = serializer.Deserialize<MasterBackupData>(reader);
                }
            }
        }

        long? IStorageCollectionController.LastContentTimeStamp => _master.LastContentTimeStamp;

        void IStorageCollectionController.UpdateContent(long lastContentTimeStamp, int folderId)
        {
            _isMasterDirty = true;

            _master.LastContentTimeStamp = lastContentTimeStamp;
            if (_master.ContentFolders == null)
            {
                _master.ContentFolders = new List<FolderTimeStampData>();
            }
            _master.ContentFolders.Add(new FolderTimeStampData
            {
                FolderId = folderId,
                TimeStamp = lastContentTimeStamp
            });
        }

        async Task IStorageCollectionController.ReleaseAsync()
        {
            if (_isMasterDirty)
            {
                await UpdateMasterAsync();
            }

            await _lease.ReleaseLeaseAsync();
        }

        private async Task UpdateMasterAsync()
        {
            var serializer = new JsonSerializer();

            using (var stringWriter = new StringWriter())
            using (var writer = new JsonTextWriter(stringWriter))
            {
                serializer.Serialize(writer, _master);

                var masterContent = stringWriter.ToString();

                await _storageFacade.UploadBlockBlobAsync(
                    Constants.BACKUP_MASTER,
                    masterContent,
                    _lease.LeaseId);
            }
        }
    }
}