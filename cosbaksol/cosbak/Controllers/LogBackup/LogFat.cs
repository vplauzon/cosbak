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
        public long LastTimeStamp
        {
            get
            {
                if (InProgressDocumentBatches.Count > 0)
                {
                    return InProgressDocumentBatches.Last().TimeStamp;
                }
                else
                {
                    return LastCheckpointTimeStamp;
                }
            }
        }

        [JsonIgnore]
        public long LastCheckpointTimeStamp
        {
            get
            {
                if (CheckPoints.Count > 0)
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
            ImmutableList<string>? idsBlockNames,
            ImmutableList<string>? sprocsBlockNames,
            ImmutableList<string>? functionsBlockNames,
            ImmutableList<string>? triggersBlockNames)
        {
            var checkpoint = new LogCheckPoint
            {
                TimeStamp = timeStamp,
                DocumentBatches = InProgressDocumentBatches,
                IdsBlockNames = idsBlockNames,
                SprocsBlockNames= sprocsBlockNames,
                FunctionsBlockNames = functionsBlockNames,
                TriggersBlockNames = triggersBlockNames
            };

            CheckPoints = CheckPoints.Add(checkpoint);
            InProgressDocumentBatches = InProgressDocumentBatches.Clear();
        }

        public IImmutableList<string> GetAllBlockNames()
        {
            var docCheckpointBlocks = CheckPoints.SelectMany(c => c.DocumentBatches.SelectMany(b => b.BlockNames));
            var docInprogressBlocks = InProgressDocumentBatches.SelectMany(b => b.BlockNames);
            var idsCheckpointBlocks = CheckPoints.SelectMany(c => c.IdsBlockNames);
            var sprocsCheckpointBlocks = CheckPoints.SelectMany(c => c.SprocsBlockNames);
            var functionsCheckpointBlocks = CheckPoints.SelectMany(c => c.FunctionsBlockNames);
            var triggersCheckpointBlocks = CheckPoints.SelectMany(c => c.TriggersBlockNames);

            return docCheckpointBlocks
                .Concat(docInprogressBlocks)
                .Concat(idsCheckpointBlocks)
                .Concat(sprocsCheckpointBlocks)
                .Concat(functionsCheckpointBlocks)
                .Concat(triggersCheckpointBlocks)
                .ToImmutableList();
        }
    }
}