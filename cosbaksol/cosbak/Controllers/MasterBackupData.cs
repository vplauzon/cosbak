using System;
using System.Collections.Generic;

namespace Cosbak.Controllers
{
    public class MasterBackupData
    {
        public long? LastContentTimeStamp { get; set; }

        public List<BackupBatchData> Batches { get; set; }
            = new List<BackupBatchData>();

        public DateTime LastIndexSync { get; set; } = DateTime.Now;
    }
}