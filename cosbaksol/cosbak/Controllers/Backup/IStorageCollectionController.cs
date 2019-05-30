using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public interface IStorageCollectionController
    {
        long? LastContentTimeStamp { get; }

        IStoragePartitionController GetPartition(string id);

        void UpdateContent(long lastContentTimeStamp);

        Task ReleaseAsync();
    }
}