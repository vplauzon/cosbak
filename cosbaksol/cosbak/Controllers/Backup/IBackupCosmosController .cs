using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IBackupCosmosController
    {
        Task<ICosmosCollectionController> GetCollectionAsync(string db, string collection);
    }
}