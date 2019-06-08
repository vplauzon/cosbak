using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public class IndexController
    {
        private readonly ILogger _logger;
        private readonly IIndexStorageController _indexStorageController;

        public IndexController(ILogger logger, IIndexStorageController indexStorageController)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _indexStorageController = indexStorageController
                ?? throw new ArgumentNullException(nameof(indexStorageController));
        }

        public async Task IndexAsync()
        {
            _logger.Display("Indexing...");
            _logger.WriteEvent("Indexing-Start");
            _logger.WriteEvent("Indexing-End");
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}