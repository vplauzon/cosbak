using System.Collections.Immutable;

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
    }
}