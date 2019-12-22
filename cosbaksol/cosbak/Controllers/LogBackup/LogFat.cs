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

        public void AddDocumentBatch(long timeStamp, IImmutableList<Block> blocks)
        {
            var batch = new DocumentBatch
            {
                TimeStamp = timeStamp,
                Blocks = blocks
            };

            InProgressDocumentBatches = InProgressDocumentBatches.Add(batch);
        }

        public void CreateCheckPoint(
            long timeStamp,
            IImmutableList<Block>? idsBlocks,
            IImmutableList<Block>? sprocsBlocks,
            IImmutableList<Block>? functionsBlocks,
            IImmutableList<Block>? triggersBlocks)
        {
            var checkpoint = new LogCheckPoint
            {
                TimeStamp = timeStamp,
                DocumentBatches = InProgressDocumentBatches,
                IdsBlocks = idsBlocks,
                SprocsBlocks = sprocsBlocks,
                FunctionsBlocks = functionsBlocks,
                TriggersBlocks = triggersBlocks
            };

            CheckPoints = CheckPoints.Add(checkpoint);
            InProgressDocumentBatches = InProgressDocumentBatches.Clear();
        }

        public IEnumerable<Block> GetInProgressDocumentsBlocks()
        {
            var docInprogressBlocks = InProgressDocumentBatches.SelectMany(b => b.Blocks);

            return docInprogressBlocks;
        }

        public IEnumerable<Block> GetCheckpointBlocks()
        {
            var idsCheckpointBlocks = CheckPoints.SelectMany(c => c.IdsBlocks);
            var sprocsCheckpointBlocks = CheckPoints.SelectMany(c => c.SprocsBlocks);
            var functionsCheckpointBlocks = CheckPoints.SelectMany(c => c.FunctionsBlocks);
            var triggersCheckpointBlocks = CheckPoints.SelectMany(c => c.TriggersBlocks);
            var docCheckpointBlocks = CheckPoints
                .SelectMany(c => c.DocumentBatches.SelectMany(b => b.Blocks));

            return idsCheckpointBlocks
                .Concat(sprocsCheckpointBlocks)
                .Concat(functionsCheckpointBlocks)
                .Concat(triggersCheckpointBlocks)
                .Concat(docCheckpointBlocks);
        }

        public IEnumerable<Block> GetAllBlocks()
        {
            return GetCheckpointBlocks()
                .Concat(GetInProgressDocumentsBlocks());
        }

        public void PurgeDocuments(long lastTimestamp)
        {
            var inProgressDocumentBatches =
                from b in InProgressDocumentBatches
                where b.TimeStamp > lastTimestamp
                select b;
            var checkPoints = from c in CheckPoints
                              select c.PurgeDocuments(lastTimestamp);

            InProgressDocumentBatches = inProgressDocumentBatches.ToImmutableList();
            CheckPoints = checkPoints.ToImmutableList();
        }
}
}