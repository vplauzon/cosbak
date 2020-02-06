using System;
using System.Collections.Immutable;
using System.Linq;

namespace Cosbak.Controllers.LogBackup
{
    public class LogCheckPoint
    {
        public long TimeStamp { get; set; }

        public ImmutableList<DocumentBatch> DocumentBatches { get; set; }
            = ImmutableList<DocumentBatch>.Empty;

        public IImmutableList<Block>? IdsBlocks { get; set; } = ImmutableList<Block>.Empty;

        public IImmutableList<Block>? SprocsBlocks { get; set; } = ImmutableList<Block>.Empty;

        public IImmutableList<Block>? FunctionsBlocks { get; set; } = ImmutableList<Block>.Empty;

        public IImmutableList<Block>? TriggersBlocks { get; set; } = ImmutableList<Block>.Empty;

        public LogCheckPoint PurgeDocuments(long lastTimestamp)
        {
            var purgedDocuments = from b in DocumentBatches
                                  where b.TimeStamp > lastTimestamp
                                  select b;

            return new LogCheckPoint
            {
                TimeStamp = TimeStamp,
                DocumentBatches = purgedDocuments.ToImmutableList(),
                IdsBlocks = IdsBlocks,
                SprocsBlocks = SprocsBlocks,
                FunctionsBlocks = FunctionsBlocks,
                TriggersBlocks = TriggersBlocks
            };
        }
    }
}