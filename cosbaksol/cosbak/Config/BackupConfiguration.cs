using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Cosbak.Config
{
    public class BackupConfiguration
    {
        public CosmosAccountConfiguration CosmosAccount { get; set; } = new CosmosAccountConfiguration();

        public StorageAccountConfiguration StorageAccount { get; set; } = new StorageAccountConfiguration();

        public BackupPlan GeneralPlan { get; set; } = new BackupPlan();

        public CollectionBackupPlanOverride[] Collections { get; set; } = new CollectionBackupPlanOverride[0];

        public void Validate()
        {
            CosmosAccount.Validate();
            StorageAccount.Validate();
            GeneralPlan.Validate();
            if (Collections.Length == 0)
            {
                throw new CosbakException("No collection defined");
            }
            foreach (var collection in Collections)
            {
                collection.Validate();
            }
        }

        public IImmutableList<CollectionBackupPlan> GetCollectionPlans()
        {
            var plans = from c in Collections
                        select new CollectionBackupPlan(
                            c.Db,
                            c.Collection,
                            OverridePlan(c.SpecificPlan));

            return plans.ToImmutableArray();
        }

        private BackupPlan OverridePlan(OverrideBackupPlan specificPlan)
        {
            return new BackupPlan
            {
                RetentionInDays = Override(GeneralPlan.RetentionInDays, specificPlan.RetentionInDays),
                RpoInSeconds = Override(GeneralPlan.RpoInSeconds, specificPlan.RpoInSeconds),
                Included = new BackupOptions
                {
                    ExplicitDelete = Override(GeneralPlan.Included.ExplicitDelete, specificPlan.Included.ExplicitDelete),
                    TtlDelete = Override(GeneralPlan.Included.TtlDelete, specificPlan.Included.TtlDelete),
                    AmbiantTtlDelete = Override(GeneralPlan.Included.AmbiantTtlDelete, specificPlan.Included.AmbiantTtlDelete),
                    Sprocs = Override(GeneralPlan.Included.Sprocs, specificPlan.Included.Sprocs),
                    Functions = Override(GeneralPlan.Included.Functions, specificPlan.Included.Functions),
                    Triggers = Override(GeneralPlan.Included.Triggers, specificPlan.Included.Triggers),
                }
            };
        }

        private static T Override<T>(T general, T? specific) where T : struct
        {
            return specific == null
                ? general
                : specific.Value;
        }
    }
}