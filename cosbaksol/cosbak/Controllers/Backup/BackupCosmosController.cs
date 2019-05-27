using Cosbak.Logging;
using Cosbak.Cosmos;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Cosbak.Controllers.Backup
{
    public class BackupCosmosController : IBackupCosmosController
    {
        private readonly IDatabaseAccountFacade _accountFacade;
        private readonly ILogger _logger;
        private readonly IImmutableList<string> _filters;

        public BackupCosmosController(
            IDatabaseAccountFacade accountFacade,
            ILogger logger,
            IEnumerable<string> filters)
        {
            _accountFacade = accountFacade ?? throw new ArgumentNullException(nameof(accountFacade));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _filters = filters == null
                ? ImmutableArray<string>.Empty
                : CleanFilters(filters);
        }

        private ImmutableArray<string> CleanFilters(IEnumerable<string> filters)
        {
            var trimmed = from f in filters
                          select f.Trim();

            return trimmed.ToImmutableArray();
        }
    }
}