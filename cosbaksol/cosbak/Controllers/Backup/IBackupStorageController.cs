using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IBackupStorageController
    {
        Task<IStorageCollectionController> LockMasterAsync(string account, string database, string collection);
    }
}