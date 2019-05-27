using Cosbak.Logging;
using Cosbak.Cosmos;
using System;
using System.Collections.Generic;

namespace Cosbak.Controllers.Backup
{
    public class BackupCosmosController : IBackupCosmosController
    {
        private readonly IDatabaseAccountFacade _accountFacade;
        private readonly ILogger _logger;

        public BackupCosmosController(IDatabaseAccountFacade accountFacade, ILogger logger)
        {
            _accountFacade = accountFacade ?? throw new ArgumentNullException(nameof(accountFacade)); ;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger)); ;
        }
    }
}