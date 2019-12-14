using Cosbak.Cosmos;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Backup
{
    public class BackupCosmosController : IBackupCosmosController
    {
        private readonly ICosmosAccountFacade _accountFacade;
        private readonly ILogger _logger;

        #region Constructors
        public BackupCosmosController(
            ICosmosAccountFacade accountFacade,
            ILogger logger)
        {
            _accountFacade = accountFacade ?? throw new ArgumentNullException(nameof(accountFacade));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }
        #endregion

        async Task<ICosmosCollectionController> IBackupCosmosController.GetCollectionAsync(
            string db,
            string collection)
        {
            var collectionFacade = await _accountFacade.GetCollectionAsync(db, collection);
            var controller = new CosmosCollectionController(collectionFacade, _logger);

            return controller;
        }
    }
}