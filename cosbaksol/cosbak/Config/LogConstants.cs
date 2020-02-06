using System;

namespace Cosbak.Config
{
    public class LogConstants
    {
        public int MaxBatchSize { get; set; } = 1000;

        public int MaxBlockCount { get; set; } = 1000;

        public int MaxDocumentSize { get; set; } = 1024 * 1024 * 1024;

        public int MaxBlockSize { get; set; } = 2 * 1024 * 1024;

        public TimeSpan MaxCheckpointAge { get; set; } = TimeSpan.FromDays(1);
    }
}