using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public interface IIndexStorageController
    {
        Task<IImmutableList<ICollectionBackupController>> GetCollectionsAsync();
    }
}