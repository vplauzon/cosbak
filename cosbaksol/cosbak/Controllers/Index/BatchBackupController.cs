using System;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cosbak.Storage;

namespace Cosbak.Controllers.Index
{
    public class BatchBackupController : IBatchBackupController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly int _folderId;
        private readonly long _timeStamp;

        public BatchBackupController(
            IStorageFacade storageFacade,
            int folderId,
            long timeStamp)
        {
            _storageFacade = storageFacade;
            _folderId = folderId;
            _timeStamp = timeStamp;
        }

        long IBatchBackupController.TimeStamp => _timeStamp;

        int IBatchBackupController.FolderId => _folderId;

        async Task<IImmutableList<IPartitionBackupController>>
            IBatchBackupController.GetPartitionsAsync()
        {
            Func<string, bool> filter = path =>
            path.EndsWith(".content") || path.EndsWith(".meta");
            var paths = await _storageFacade.ListBlobsAsync(filter);
            var partitionIds = from p in paths
                               let parts = p.Split('.')
                               //   Only path with one dot
                               where parts.Length == 2
                               let partition = parts[0]
                               let extension = parts[1]
                               group extension by partition into g
                               //   Only files coming in pair "meta" + "content"
                               where g.Count() == 2
                               select g.Key;
            var partitions = from id in partitionIds
                             select new PartitionBackupController(_storageFacade, id);

            return partitions.Cast<IPartitionBackupController>().ToImmutableArray();
        }
    }
}