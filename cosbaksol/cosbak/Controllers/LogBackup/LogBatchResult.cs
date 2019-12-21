using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers.LogBackup
{
    internal class LogBatchResult
    {
        public LogBatchResult(bool hasCaughtUp)
        {
            HasCaughtUp = hasCaughtUp;
        }

        public bool HasCaughtUp { get; }
    }
}