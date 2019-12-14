using Cosbak.Config;
using Cosbak.Controllers.Log;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Threading.Tasks;

namespace Cosbak.Controllers
{
    internal class BackupScheduler
    {
        #region Inner Types
        private class CollectionPlan
        {
            public CollectionPlan(LogCollectionBackupController collectionController, BackupPlan plan)
            {
                CollectionController = collectionController;
                Plan = plan;
            }

            public LogCollectionBackupController CollectionController { get; }

            public BackupPlan Plan { get; }
        }

        private class Initialized
        {
            public Initialized(IEnumerable<CollectionPlan> collectionPlans)
            {
                CollectionPlans = collectionPlans.ToImmutableArray();
            }

            public IImmutableList<CollectionPlan> CollectionPlans { get; }
        }
        #endregion

        private readonly ILogger _logger;
        private readonly ICosmosAccountFacade _cosmosFacade;
        private readonly IStorageFacade _storageFacade;
        private IImmutableList<CollectionBackupPlan> _collectionBackupPlans;
        private Initialized? _initialized;

        public BackupScheduler(
            ILogger logger,
            ICosmosAccountFacade cosmosFacade,
            IStorageFacade storageFacade,
            IImmutableList<CollectionBackupPlan> collectionPlans)
        {
            _logger = logger;
            _cosmosFacade = cosmosFacade;
            _storageFacade = storageFacade;
            _collectionBackupPlans = collectionPlans;
        }

        public async Task InitializeAsync()
        {
            if (_initialized != null || _collectionBackupPlans == null)
            {
                throw new InvalidOperationException("InitializeAsync has already been called");
            }

            var collectionPlans = await GetCollectionPlansAsync(
                _collectionBackupPlans,
                _cosmosFacade,
                _storageFacade,
                _logger).ToEnumerable();

            _initialized = new Initialized(collectionPlans);
        }

        public async Task ProcessIterationAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            _logger.Display("Backup Iteration...");
            _logger.WriteEvent("Backup-Iteration-Start");
            foreach (var plan in _initialized.CollectionPlans)
            {
                await plan.CollectionController.BackupBatchAsync();
            }
            _logger.WriteEvent("Backup-Iteration-End");
        }

        public Task ProcessContinuouslyAsync()
        {
            throw new NotImplementedException();
        }

        private async static IAsyncEnumerable<CollectionPlan> GetCollectionPlansAsync(
            IImmutableList<CollectionBackupPlan> collectionBackupPlans,
            ICosmosAccountFacade cosmosFacade,
            IStorageFacade storageFacade,
            ILogger logger)
        {
            var byDbs = collectionBackupPlans.GroupBy(p => p.Db).ToDictionary(g => g.Key);
            var dbs = await cosmosFacade.GetDatabasesAsync();

            logger = logger.AddContext("Account", cosmosFacade.AccountName);
            foreach (var db in dbs)
            {
                if (byDbs.ContainsKey(db.DatabaseName))
                {
                    var byCollections = byDbs[db.DatabaseName].ToDictionary(c => c.Collection);
                    var collections = await db.GetCollectionsAsync();

                    foreach (var coll in collections)
                    {
                        if (byCollections.ContainsKey(coll.CollectionName))
                        {
                            var plan = byCollections[coll.CollectionName].SpecificPlan;
                            var collectionController = new LogCollectionBackupController(
                                coll,
                                storageFacade,
                                logger
                                .AddContext("Db", db.DatabaseName)
                                .AddContext("Collection", coll.CollectionName));

                            yield return new CollectionPlan(collectionController, plan);
                        }
                    }
                }
            }
        }
    }
}