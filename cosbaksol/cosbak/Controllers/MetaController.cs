using Cosbak.Commands;
using Cosbak.Config;
using Cosbak.Controllers.Index;
using Cosbak.Controllers.Restore;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;

namespace Cosbak.Controllers
{
    internal class MetaController
    {
        public async Task BackupAsync(BackupConfiguration configuration, BackupMode mode)
        {
            var watch = new Stopwatch();

            watch.Start();
            configuration.Validate();

            var storageFacade = CreateStorageFacade(configuration.StorageAccount);
            ILogger logger = new StorageFolderLogger(storageFacade.ChangeFolder("logs"));
            var cosmosFacade = CreateCosmosFacade(configuration.CosmosAccount, logger);
            var scheduler = new BackupScheduler(
                logger,
                cosmosFacade,
                storageFacade,
                configuration.GetCollectionPlans(),
                configuration.Constants);

            try
            {
                await scheduler.InitializeAsync();
                try
                {
                    if (mode == BackupMode.Iterative)
                    {
                        await scheduler.ProcessIterationAsync();
                    }
                    else
                    {
                        await scheduler.ProcessContinuouslyAsync();
                    }
                    logger.Display($"Elapsed Time:  {watch.Elapsed}");
                    logger.Display("Memory used:  "
                        + $"{Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024} Mb");
                }
                finally
                {
                    await Task.WhenAll(scheduler.DisposeAsync(), logger.FlushAsync());
                }
            }
            catch (Exception ex)
            {
                logger.DisplayError(ex);
            }
        }

        public async Task RestoreAsync(
            RestoreConfiguration configuration,
            DateTime? pointInTime = null)
        {
            var watch = new Stopwatch();

            watch.Start();
            configuration.Validate();

            var storageFacade = CreateStorageFacade(configuration.StorageAccount);
            ILogger logger = new StorageFolderLogger(storageFacade.ChangeFolder("logs"));
            var cosmosFacade = CreateCosmosFacade(
                configuration.CosmosAccount,
                logger) as ICosmosAccountFacade;
            var sourceCollectionLogger = logger
                .AddContext("Db", configuration.SourceCollection.Db)
                .AddContext("Collection", configuration.SourceCollection.Collection);
            var indexController = new IndexCollectionController(
                configuration.SourceCollection.Account,
                configuration.SourceCollection.Db,
                configuration.SourceCollection.Collection,
                storageFacade,
                null,
                configuration.Constants.IndexConstants,
                sourceCollectionLogger);

            try
            {
                await indexController.InitializeAsync();
                try
                {
                    if (pointInTime == null)
                    {
                        await indexController.LoadAsync(true);
                    }
                    else
                    {
                        await indexController.LoadUntilAsync(pointInTime);
                    }

                    var collection = await FindOrCreateCollectionAsync(
                        cosmosFacade,
                        configuration.TargetCollection);
                    var targetCollectionLogger = logger
                        .AddContext("Db", configuration.TargetCollection.Db)
                        .AddContext("Collection", configuration.TargetCollection.Collection);
                    var restoreController = new RestoreController(
                        configuration.SourceCollection.Account,
                        configuration.SourceCollection.Db,
                        configuration.SourceCollection.Collection,
                        storageFacade,
                        collection,
                        targetCollectionLogger);

                    await restoreController.InitializeAsync();
                    try
                    {
                        await restoreController.RestoreAsync(pointInTime);
                        logger.Display($"Elapsed Time:  {watch.Elapsed}");
                        logger.Display("Memory used:  "
                            + $"{Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024} Mb");
                    }
                    finally
                    {
                        await restoreController.DisposeAsync();
                    }
                }
                finally
                {
                    await Task.WhenAll(indexController.DisposeAsync(), logger.FlushAsync());
                }
            }
            catch (Exception ex)
            {
                logger.DisplayError(ex);
            }
        }

        private async Task<ICollectionFacade> FindOrCreateCollectionAsync(
            ICosmosAccountFacade cosmosFacade,
            CollectionConfiguration targetCollection)
        {
            var dbs = await cosmosFacade.GetDatabasesAsync();
            var db = dbs.Where(d => d.DatabaseName == targetCollection.Db).FirstOrDefault();

            if (db == null)
            {
                throw new CosbakException($"Can't find database {targetCollection.Db}");
            }
            else
            {
                var collections = await db.GetCollectionsAsync();
                var collection = collections
                    .Where(c => c.CollectionName == targetCollection.Collection)
                    .FirstOrDefault();

                if (collection == null)
                {
                    throw new CosbakException($"Can't find collection {targetCollection.Collection}");
                }
                else
                {
                    return collection;
                }
            }
        }

        private static IStorageFacade CreateStorageFacade(StorageAccountConfiguration description)
        {
            if (!string.IsNullOrWhiteSpace(description.Token))
            {
                return StorageFacade.FromToken(
                    description.Name,
                    description.Container,
                    description.Folder,
                    description.Token);
            }
            else if (!string.IsNullOrWhiteSpace(description.TokenPath))
            {
                throw new NotImplementedException();
            }
            else if (!string.IsNullOrWhiteSpace(description.Key))
            {
                return StorageFacade.FromKey(
                    description.Name,
                    description.Container,
                    description.Folder,
                    description.Key);
            }
            else if (!string.IsNullOrWhiteSpace(description.KeyPath))
            {
                throw new NotImplementedException();
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        private static CosmosAccountFacade CreateCosmosFacade(
            CosmosAccountConfiguration configuration,
            ILogger logger)
        {
            return new CosmosAccountFacade(
                configuration.Name,
                configuration.Key,
                logger);
        }
    }
}