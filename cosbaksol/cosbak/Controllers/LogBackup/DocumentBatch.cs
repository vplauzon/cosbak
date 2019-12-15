﻿using System.Collections.Immutable;
using YamlDotNet.Serialization.NodeTypeResolvers;

namespace Cosbak.Controllers.LogBackup
{
    public class DocumentBatch
    {
        public long TimeStamp { get; set; }

        public ImmutableList<string> BlockNames { get; set; } = ImmutableList<string>.Empty;
    }
}