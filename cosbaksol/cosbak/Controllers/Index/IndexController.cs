using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public class IndexController
    {
        private readonly ILogger _logger;

        public IndexController(ILogger logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
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