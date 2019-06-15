using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers
{
    internal static class Constants
    {
        public const string ACCOUNTS_FOLDER = "accounts";

        public const string BACKUP_FOLDER = "raw-backup";

        public const string BACKUP_MASTER = "master.json";

        public const string INDEX_FOLDER = "indexed-backup";

        public const int MAX_INDEX_LENGTH = 2048;

        public const int MAX_CONTENT_LENGTH = 2048;
    }
}