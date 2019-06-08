using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading.Tasks;
using Cosbak.Storage;

namespace Cosbak.Controllers.Index
{
    public class IndexStorageController : IIndexStorageController
    {
        private readonly IStorageFacade _storageFacade;
        private readonly ILogger _logger;

        public IndexStorageController(IStorageFacade storageFacade, ILogger logger)
        {
            _storageFacade = storageFacade ?? throw new ArgumentNullException(nameof(storageFacade));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        Task<IImmutableList<IIndexCollectionBackupController>> IIndexStorageController.GetCollectionsAsync()
        {
            throw new NotImplementedException();
        }
    }
}