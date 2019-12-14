using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IBackupStorageController
    {
        Task<IStorageCollectionController> LockLogBlobAsync(
            string account,
            string database,
            string collection);
    }
}