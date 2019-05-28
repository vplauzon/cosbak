using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IBackupCosmosController
    {
        Task<IImmutableList<ICosmosCollectionController>> GetCollectionsAsync();
    }
}