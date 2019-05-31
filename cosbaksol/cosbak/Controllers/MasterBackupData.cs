using System;
using System.Collections.Generic;

namespace Cosbak.Controllers
{
    public class MasterBackupData
    {
        public long? LastContentTimeStamp { get; set; }

        public List<FolderTimeStampData> ContentFolders { get; set; }

        public DateTime? LastIndexSync { get; set; }

        public void ApplyDefaults()
        {
            if (ContentFolders == null)
            {
                ContentFolders = new List<FolderTimeStampData>();
            }
        }
    }
}