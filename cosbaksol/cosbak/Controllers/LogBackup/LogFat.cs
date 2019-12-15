using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.Json.Serialization;

namespace Cosbak.Controllers.LogBackup
{
    internal class LogFat
    {
        public ImmutableList<DocumentBatch> InProgressDocumentBatches { get; set; }
            = ImmutableList<DocumentBatch>.Empty;

        public ImmutableList<LogCheckPoint> CheckPoints { get; set; }
            = ImmutableList<LogCheckPoint>.Empty;

        [JsonIgnore]
        public long LastUpdateTime
        {
            get
            {
                if (InProgressDocumentBatches.Count > 0)
                {
                    return InProgressDocumentBatches.Last().LastUpdateTime;
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

        public void AddDocumentBatch(long lastUpdateTime, ImmutableList<string> blockNames)
        {
            var batch = new DocumentBatch
            {
                LastUpdateTime = lastUpdateTime,
                BlockNames = blockNames
            };

            InProgressDocumentBatches = InProgressDocumentBatches.Add(batch);
        }

        public IImmutableList<string> GetAllBlockNames()
        {
            var inprogressBlocks = InProgressDocumentBatches.SelectMany(b => b.BlockNames);
            var checkpointBlocks = CheckPoints.SelectMany(c => c.DocumentBatches.SelectMany(b => b.BlockNames));

            return inprogressBlocks.Concat(checkpointBlocks).ToImmutableList();
        }
    }
}