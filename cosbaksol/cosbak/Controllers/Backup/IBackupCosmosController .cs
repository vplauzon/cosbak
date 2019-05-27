using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IBackupCosmosController
    {
        Task<IEnumerable<ICosmosCollectionController>> GetCollectionsAsync();
    }
}