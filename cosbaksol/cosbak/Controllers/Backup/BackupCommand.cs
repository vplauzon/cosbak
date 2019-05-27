using Cosbak.Config;
using Cosbak.Controllers.Backup;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Cosbak.Controllers.Backup
{
    public class BackupCommand : CommandBase<BackupDescription>
    {
        public BackupCommand() : base(CreateSubSections, CreateSwitchToAction())
        {
        }

        private static void CreateSubSections(BackupDescription description)
        {
            if (description.CosmosAccount == null)
            {
                description.CosmosAccount = new CosmosAccountDescription();
                description.StorageAccount = new StorageAccountDescription();
            }
        }

        private static IImmutableDictionary<string, Action<BackupDescription, string>> CreateSwitchToAction()
        {
            var switchToAction = ImmutableSortedDictionary<string, Action<BackupDescription, string>>
                .Empty
                .Add("cn", (c, a) => c.CosmosAccount.Name = a)
                .Add("ck", (c, a) => c.CosmosAccount.Key = a)
                .Add("sn", (c, a) => c.StorageAccount.Name = a)
                .Add("sc", (c, a) => c.StorageAccount.Container = a)
                .Add("sf", (c, a) => c.StorageAccount.Folder = a)
                .Add("sk", (c, a) => c.StorageAccount.Key = a)
                .Add("st", (c, a) => c.StorageAccount.Token = a);

            return switchToAction;
        }
    }
}