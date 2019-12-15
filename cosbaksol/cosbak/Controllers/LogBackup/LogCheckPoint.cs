﻿using System.Collections.Immutable;

namespace Cosbak.Controllers.LogBackup
{
    public class LogCheckPoint
    {
        public int CheckPointTime { get; set; }

        public ImmutableList<DocumentBatch> DocumentBatches { get; set; }
            = ImmutableList<DocumentBatch>.Empty;
    }
}