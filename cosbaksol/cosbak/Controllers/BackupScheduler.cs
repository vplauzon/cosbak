using Cosbak.Config;
using Cosbak.Controllers.Index;
using Cosbak.Controllers.LogBackup;
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
            public CollectionPlan(
                LogCollectionBackupController logController,
                IndexCollectionController indexController,
                BackupPlan plan)
            {
                LogController = logController;
                IndexController = indexController;
                Plan = plan;
            }

            public LogCollectionBackupController LogController { get; }

            public IndexCollectionController IndexController { get; }

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
        private readonly IImmutableList<CollectionBackupPlan> _collectionBackupPlans;
        private readonly TechnicalConstants _technicalConstants;
        private Initialized? _initialized;

        public BackupScheduler(
            ILogger logger,
            ICosmosAccountFacade cosmosFacade,
            IStorageFacade storageFacade,
            IImmutableList<CollectionBackupPlan> collectionPlans,
            TechnicalConstants technicalConstants)
        {
            _logger = logger;
            _cosmosFacade = cosmosFacade;
            _storageFacade = storageFacade;
            _collectionBackupPlans = collectionPlans;
            _technicalConstants = technicalConstants;
        }

        public async Task InitializeAsync()
        {
            if (_initialized != null || _collectionBackupPlans == null)
            {
                throw new InvalidOperationException("InitializeAsync has already been called");
            }

            var collectionPlans = await GetCollectionPlansAsync(
                _collectionBackupPlans,
                _technicalConstants,
                _cosmosFacade,
                _storageFacade,
                _logger).ToEnumerable();
            var initializeControllerTasks = from p in collectionPlans
                                            select p.LogController.InitializeAsync();

            await Task.WhenAll(initializeControllerTasks);
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
                while (true)
                {
                    var result = await plan.LogController.LogBatchAsync();

                    if (result.NeedDocumentsPurge || result.NeedCheckpointPurge)
                    {
                        var lastTimestamp =
                            await plan.IndexController.IndexAsync(result.NeedCheckpointPurge);

                        await plan.LogController.PurgeAsync(
                            !result.NeedCheckpointPurge,
                            lastTimestamp);
                    }
                    if (result.HasCaughtUp)
                    {
                        break;
                    }
                }
            }
            _logger.WriteEvent("Backup-Iteration-End");
        }

        public Task ProcessContinuouslyAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            throw new NotImplementedException();
        }

        public async Task DisposeAsync()
        {
            if (_initialized == null)
            {
                throw new InvalidOperationException("InitializeAsync hasn't been called");
            }

            var disposeControllerTasks = from p in _initialized.CollectionPlans
                                         select p.LogController.DisposeAsync();

            await Task.WhenAll(disposeControllerTasks);
        }

        private async static IAsyncEnumerable<CollectionPlan> GetCollectionPlansAsync(
            IImmutableList<CollectionBackupPlan> collectionBackupPlans,
            TechnicalConstants technicalConstants,
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
                            var collectionLogger = logger
                                .AddContext("Db", db.DatabaseName)
                                .AddContext("Collection", coll.CollectionName);
                            var logController = new LogCollectionBackupController(
                                coll,
                                storageFacade,
                                plan.Rpo,
                                plan.Included,
                                technicalConstants.LogConstants,
                                collectionLogger);
                            var indexController = new IndexCollectionController(
                                coll,
                                storageFacade,
                                plan.RetentionInDays,
                                technicalConstants.IndexConstants,
                                collectionLogger);

                            yield return new CollectionPlan(logController, indexController, plan);
                        }
                    }
                }
            }
        }
    }
}