using System;
using System.Collections.Generic;
using System.Text;
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
    }
}