using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text.Json;

namespace Cosbak.Controllers.Index
{
    internal class LogSprocBatch: LogItemBatch<StoredProcedure>
    {
        public LogSprocBatch(long batchTimeStamp, ReadOnlySequence<byte> sequence)
            : base(batchTimeStamp, sequence)
        {
        }

        protected override string ItemProperty => "StoredProcedures";

        protected override StoredProcedure TransformElement(JsonElement element)
        {
            var id = element.GetProperty("id").GetString();
            var timeStamp = element.GetProperty("_ts").GetInt64();
            var body = element.GetProperty("body").GetString();

            return new StoredProcedure(id, timeStamp, body);
        }
    }
}