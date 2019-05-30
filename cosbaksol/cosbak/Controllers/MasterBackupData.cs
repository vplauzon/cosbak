using System;
using System.Collections.Generic;

namespace Cosbak.Controllers
{
    public class MasterBackupData
    {
        public long? LastContentTimeStamp { get; set; }

        public List<FolderTimeStampData> ContentFolders { get; set; }

        public DateTime LastClean { get; set; } = DateTime.Now;
    }
}