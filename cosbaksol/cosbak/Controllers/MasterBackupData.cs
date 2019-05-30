using System;
using System.Collections.Generic;

namespace Cosbak.Controllers
{
    public class MasterBackupData
    {
        public long? LastContentTimeStamp { get; set; }

        public List<FolderTimeStampData> ContentFolders { get; set; } = new List<FolderTimeStampData>();

        public DateTime LastIndexSync { get; set; } = DateTime.Now;
    }
}