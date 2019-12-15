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
                    return InProgressDocumentBatches.Last().TimeStamp;
                }
                else if (CheckPoints.Count > 0)
                {
                    return CheckPoints.Last().TimeStamp;
                }
                else
                {
                    return 0;
                }
            }
        }

        public void AddDocumentBatch(long timeStamp, ImmutableList<string> blockNames)
        {
            var batch = new DocumentBatch
            {
                TimeStamp = timeStamp,
                BlockNames = blockNames
            };

            InProgressDocumentBatches = InProgressDocumentBatches.Add(batch);
        }

        public void CreateCheckPoint(
            long timeStamp,
            ImmutableList<string>? idsBlockNames)
        {
            var checkpoint = new LogCheckPoint
            {
                TimeStamp = timeStamp,
                DocumentBatches = InProgressDocumentBatches,
                IdsBlockNames = idsBlockNames
            };

            CheckPoints = CheckPoints.Add(checkpoint);
            InProgressDocumentBatches = InProgressDocumentBatches.Clear();
        }

        public IImmutableList<string> GetAllBlockNames()
        {
            var checkpointBlocks = CheckPoints.SelectMany(c => c.DocumentBatches.SelectMany(b => b.BlockNames));
            var inprogressBlocks = InProgressDocumentBatches.SelectMany(b => b.BlockNames);

            return checkpointBlocks.Concat(inprogressBlocks).ToImmutableList();
        }
    }
}