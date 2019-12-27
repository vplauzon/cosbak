using Cosbak.Commands;
using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

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
            var cosmosFacade = new CosmosAccountFacade(
                configuration.CosmosAccount.Name,
                configuration.CosmosAccount.Key,
                logger);
            var scheduler = new BackupScheduler(
                logger,
                cosmosFacade,
                storageFacade,
                configuration.GetCollectionPlans(),
                configuration.Constants);

            try
            {
                await scheduler.InitializeAsync();
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
            catch (Exception ex)
            {
                logger.DisplayError(ex);
            }
            finally
            {
                await Task.WhenAll(scheduler.DisposeAsync(), logger.FlushAsync());
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
    }
}