using System;

namespace Cosbak.Config
{
    public class LogConstants
    {
        public int MaxBatchSize { get; set; } = 2;

        public int MaxBlockCount { get; set; } = 1000;

        public long MaxDocumentSize { get; set; } = 1024 * 1024 * 1024;

        public TimeSpan MaxCheckpointAge { get; set; } = TimeSpan.FromDays(1);
    }
}