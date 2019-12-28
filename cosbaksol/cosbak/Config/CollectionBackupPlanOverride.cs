using System;

namespace Cosbak.Config
{
    public class CollectionBackupPlanOverride : CollectionConfiguration
    {
        public OverrideBackupPlan SpecificPlan { get; set; } = new OverrideBackupPlan();

        public new void Validate()
        {
            base.Validate();

            SpecificPlan.Validate();
        }
    }
}