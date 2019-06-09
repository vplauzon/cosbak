using Cosbak.Config;
using Cosbak.Controllers.Backup;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Cosbak.Controllers.Backup
{
    public class IndexCommand : CommandBase<IndexDescription>
    {
        public IndexCommand() : base(CreateSubSections, CreateSwitchToAction())
        {
        }

        private static void CreateSubSections(IndexDescription description)
        {
            if (description.StorageAccount == null)
            {
                description.StorageAccount = new StorageAccountDescription();
            }
        }

        private static IImmutableDictionary<string, Action<IndexDescription, string>> CreateSwitchToAction()
        {
            var switchToAction = ImmutableSortedDictionary<string, Action<IndexDescription, string>>
                .Empty
                .Add("sn", (c, a) => c.StorageAccount.Name = a)
                .Add("sc", (c, a) => c.StorageAccount.Container = a)
                .Add("sf", (c, a) => c.StorageAccount.Folder = a)
                .Add("sk", (c, a) => c.StorageAccount.Key = a)
                .Add("st", (c, a) => c.StorageAccount.Token = a);

            return switchToAction;
        }
    }
}