using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers.LogBackup
{
    internal class LogBatchResult
    {
        public LogBatchResult(
            bool hasCaughtUp,
            bool needDocumentsPurge,
            bool needCheckpointPurge)
        {
            HasCaughtUp = hasCaughtUp;
            NeedDocumentsPurge = needDocumentsPurge;
            NeedCheckpointPurge = needCheckpointPurge;
        }

        public bool HasCaughtUp { get; }
        
        public bool NeedDocumentsPurge { get; }
        
        public bool NeedCheckpointPurge { get; }
    }
}