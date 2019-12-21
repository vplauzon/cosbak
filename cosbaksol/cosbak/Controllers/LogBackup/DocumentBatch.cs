using System.Collections.Immutable;
using YamlDotNet.Serialization.NodeTypeResolvers;

namespace Cosbak.Controllers.LogBackup
{
    public class DocumentBatch
    {
        public long TimeStamp { get; set; }

        public IImmutableList<Block> Blocks { get; set; } = ImmutableList<Block>.Empty;
    }
}