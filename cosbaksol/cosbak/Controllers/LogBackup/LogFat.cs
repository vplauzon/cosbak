using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;

namespace Cosbak.Controllers.LogBackup
{
    internal class LogFat
    {
        public ImmutableList<DocumentBatch> UncheckedDocumentBatches { get; set; }
            = ImmutableList<DocumentBatch>.Empty;

        public ImmutableList<LogCheckPoint> CheckPoints { get; set; }
            = ImmutableList<LogCheckPoint>.Empty;

        [JsonIgnore]
        public int LastUpdateTime
        {
            get
            {
                if (UncheckedDocumentBatches.Count > 0)
                {
                    return UncheckedDocumentBatches.Last().LastUpdateTime;
                }
                else if (CheckPoints.Count > 0)
                {
                    return CheckPoints.Last().CheckPointTime;
                }
                else
                {
                    return 0;
                }
            }
        }
    }
}