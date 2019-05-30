using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IStorageCollectionController
    {
        MasterBackupData MasterData { get; }

        Task UpdateMasterAsync();

        Task ReleaseAsync();
    }
}