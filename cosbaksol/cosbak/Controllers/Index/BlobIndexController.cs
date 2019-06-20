using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading.Tasks;
using Cosbak.Storage;
using Newtonsoft.Json;

namespace Cosbak.Controllers.Index
{
    public class BlobIndexController : IBlobIndexController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly BlobLease _masterLease;
        private readonly long _startTimeStamp;

        #region Constructors
        public async static Task<IBlobIndexController> GetCurrentOrNewAsync(
            IStorageFacade storageFacade,
            long startTimeStamp)
        {
            var masterLease = await GetMasterIndexLeaseAsync(storageFacade);
            var master = await GetMasterIndexAsync(storageFacade);

            await CleanMasterFolderAsync(storageFacade, master.IndexList);
            if (master.IndexList.Any())
            {
                var maxIndex = master.IndexList.Last();
                var folderName = maxIndex.StartTimeStamp.ToString("D19");
                var indexStorage = storageFacade.ChangeFolder(folderName);

                return await LoadIndexAsync(indexStorage, masterLease, maxIndex.HeaderLength);
            }
            else
            {
                return new BlobIndexController(storageFacade, masterLease, startTimeStamp);
            }
        }

        private BlobIndexController(
            IStorageFacade storageFacade,
            BlobLease masterLease,
            long startTimeStamp)
        {
            _storageFacade = storageFacade;
            _masterLease = masterLease;
            _startTimeStamp = startTimeStamp;
        }

        private static Task CleanMasterFolderAsync(
            IStorageFacade storageFacade,
            List<IndexData> indexList)
        {
            throw new NotImplementedException();
        }

        private async static Task<BlobLease> GetMasterIndexLeaseAsync(IStorageFacade storageFacade)
        {
            if (await storageFacade.DoesExistAsync(Constants.INDEX_MASTER))
            {   //  Create empty master
                await storageFacade.UploadBlockBlobAsync(Constants.INDEX_MASTER, string.Empty);
            }

            var lease = await storageFacade.GetLeaseAsync(Constants.INDEX_MASTER);

            if (lease == null)
            {
                throw new CosbakException($"Can't lease '{Constants.INDEX_MASTER}' blob");
            }
            else
            {
                return lease;
            }
        }

        private async static Task<MasterIndexData> GetMasterIndexAsync(IStorageFacade storageFacade)
        {
            var masterContent = await storageFacade.GetContentAsync(Constants.BACKUP_MASTER);
            var serializer = new JsonSerializer();

            using (var stringReader = new StringReader(masterContent))
            using (var reader = new JsonTextReader(stringReader))
            {
                var master = serializer.Deserialize<MasterIndexData>(reader)
                    ?? new MasterIndexData();

                return master;
            }
        }

        private async static Task<IBlobIndexController> LoadIndexAsync(
            IStorageFacade indexStorage,
            BlobLease masterLease,
            long headerLength)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
        #endregion

        Task IBlobIndexController.AppendAsync(Memory<byte> index)
        {
            throw new NotImplementedException();
        }
    }
}