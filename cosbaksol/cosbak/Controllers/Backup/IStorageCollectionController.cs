using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IStorageCollectionController
    {
        long? LastContentTimeStamp { get; }

        void UpdateContent(long lastContentTimeStamp, int folderId);

        Task ReleaseAsync();
    }
}