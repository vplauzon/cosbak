using System;

namespace Cosbak.Config
{
    public class CollectionBackupPlan
    {
        public CollectionBackupPlan(string db, string collection, BackupPlan specificPlan)
        {
            Db = db;
            Collection = collection;
            SpecificPlan = specificPlan;
        }

        public string Db { get; }
        
        public string Collection { get; }

        public BackupPlan SpecificPlan { get; }
    }
}