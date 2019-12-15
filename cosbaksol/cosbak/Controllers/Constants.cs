using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers
{
    internal static class Constants
    {
        public const string BACKUPS_FOLDER = "backups";

        public const string LOG_EXTENSION = "log";

        public const string INDEX_EXTENSION = "index";

        public const int MAX_LOG_BLOCK_SIZE = 2 * 1024 * 1024;
    }
}