using Cosbak.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Cosbak.Controllers.Index
{
    internal class LogDocumentBatch : LogItemBatch<JsonElement>
    {
        public LogDocumentBatch(long batchTimeStamp, ReadOnlySequence<byte> sequence)
            : base(batchTimeStamp, sequence)
        {
        }

        protected override string ItemProperty => "Documents";

        protected override JsonElement TransformElement(JsonElement element)
        {
            return element;
        }
    }
}