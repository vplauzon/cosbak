﻿using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.LogBackup
{
    internal class LogCollectionBackupController
    {
        private readonly ICollectionFacade _collection;
        private readonly LogFile _logFile;
        private readonly TimeSpan _rpo;
        private readonly BackupOptions _included;
        private readonly LogConstants _logConstants;
        private readonly ILogger _logger;

        public LogCollectionBackupController(
            ICollectionFacade collectionFacade,
            IStorageFacade storageFacade,
            TimeSpan rpo,
            BackupOptions included,
            LogConstants logConstants,
            ILogger logger)
        {
            _collection = collectionFacade;
            _logFile = new LogFile(
                storageFacade,
                collectionFacade.Parent.Parent.AccountName,
                collectionFacade.Parent.DatabaseName,
                collectionFacade.CollectionName,
                logger);
            _rpo = rpo;
            _included = included;
            _logConstants = logConstants;
            _logger = logger;
        }

        public async Task InitializeAsync(string partitionPath)
        {
            await _logFile.InitializeAsync(partitionPath);
        }

        public async Task<LogBatchResult> LogBatchAsync()
        {
            _logger.Display(
                $"Backup {_collection.Parent.DatabaseName}.{_collection.CollectionName}...");
            _logger.WriteEvent("Backup-Collection-Start");

            var previousTimeStamp = _logFile.LastTimeStamp;
            var timeWindow = await _collection.SizeTimeWindowAsync(
                previousTimeStamp,
                _logConstants.MaxBatchSize);

            _logger
                .AddContext("CurrentTimeStamp", timeWindow.currentTimeStamp)
                .AddContext("DocumentCount", timeWindow.count)
                .AddContext("MaxTimeStamp", timeWindow.maxTimeStamp)
                .WriteEvent("LogBatchTimeWindow");
            if (timeWindow.count != 0)
            {
                await LogDocumentBatchAsync(previousTimeStamp, timeWindow.maxTimeStamp);
            }
            else
            {
                _logger.Display("No document to backup");
                _logger.WriteEvent("No-document-to-backup");
            }
            var hasCaughtUp = timeWindow.currentTimeStamp == timeWindow.maxTimeStamp;
            var delta = TimeSpan.FromSeconds(
                timeWindow.currentTimeStamp
                - _logFile.LastCheckpointTimeStamp);
            var isCheckpoint = delta >= _rpo;

            if (hasCaughtUp && isCheckpoint)
            {
                await LogCheckPointAsync(timeWindow.currentTimeStamp);
            }
            await _logFile.PersistAsync();

            var needDocumentsPurge = _logFile.TotalDocumentBlockCount > _logConstants.MaxBlockCount
                || _logFile.TotalDocumentSize > _logConstants.MaxDocumentSize;
            var checkPointAge = _logFile.GetOldestCheckpointAge(timeWindow.currentTimeStamp);
            var needCheckpointPurge =
                _logFile.TotalCheckPointBlockCount > _logConstants.MaxBlockCount
                || _logFile.TotalCheckPointSize > _logConstants.MaxDocumentSize
                || checkPointAge > _logConstants.MaxCheckpointAge;

            _logger.WriteEvent("Backup-Collection-End");

            return new LogBatchResult(hasCaughtUp, needDocumentsPurge, needCheckpointPurge);
        }

        public async Task PurgeAsync(bool isDocumentOnlyPurge, long lastTimestamp)
        {
            _logger.Display(
                $"Purging logs of {_collection.Parent.DatabaseName}.{_collection.CollectionName}...");

            _logFile.Purge(isDocumentOnlyPurge, lastTimestamp);
            await _logFile.PersistAsync();
        }

        public async Task DisposeAsync()
        {
            await _logFile.DisposeAsync();
        }

        private async Task LogCheckPointAsync(long currentTimeStamp)
        {
            _logger.Display("Preparing Checkpoint...");
            _logger.WriteEvent("Checkpoint-Start");

            var idsBlockNames = _included.ExplicitDelete
                ? await WriteIteratorToBlocksAsync(_collection.GetAllIds(), "LogAllIds")
                : null;
            var sprocsBlockNames = _included.Sprocs
                ? await WriteIteratorToBlocksAsync(_collection.GetAllStoredProcedures(), "LogAllSprocs")
                : null;
            var functionsBlockNames = _included.Functions
                ? await WriteIteratorToBlocksAsync(_collection.GetAllFunctions(), "LogAllFunctions")
                : null;
            var triggersBlockNames = _included.Triggers
                ? await WriteIteratorToBlocksAsync(_collection.GetAllTriggers(), "LogAllTriggers")
                : null;

            _logFile.CreateCheckpoint(
                currentTimeStamp,
                idsBlockNames,
                sprocsBlockNames,
                functionsBlockNames,
                triggersBlockNames);
            _logger.WriteEvent("Checkpoint-End");
        }

        private async Task LogDocumentBatchAsync(long previousLastUpdateTime, long maxTimeStamp)
        {
            var iterator = _collection.GetTimeWindowDocuments(
                previousLastUpdateTime,
                maxTimeStamp);
            var blocks = await WriteIteratorToBlocksAsync(iterator, "LogDocuments");

            _logFile.AddDocumentBatch(maxTimeStamp, blocks);
        }

        private async Task<IImmutableList<Block>> WriteIteratorToBlocksAsync(
            StreamIterator iterator,
            string eventName)
        {
            _logger.Display($"{eventName}...");
            _logger.WriteEvent($"{eventName}-Start");
            using (var buffer = BufferPool.Rent(_logConstants.MaxBlockSize))
            {
                var blocks = ImmutableList<Block>.Empty;
                double ru = 0;
                var cosmosPageCount = 0;
                int index = 0;
                int totalWrites = 0;

                while (iterator.HasMoreResults)
                {
                    var result = await iterator.ReadNextAsync();

                    if (result.Stream.Length > buffer.Buffer.Length)
                    {
                        throw new NotSupportedException(
                            $"Query return bigger than buffer:  {result.Stream.Length}");
                    }
                    if (result.Stream.Length > buffer.Buffer.Length - index)
                    {
                        var block = await _logFile.WriteBlockAsync(buffer.Buffer, index);

                        totalWrites += index;
                        blocks = blocks.Add(block);
                        index = 0;
                    }

                    var memory = new Memory<byte>(buffer.Buffer, index, (int)result.Stream.Length);

                    await result.Stream.ReadAsync(memory);
                    index += memory.Length;
                    ++cosmosPageCount;
                    ru += result.RequestCharge;
                }
                if (index > 0)
                {
                    var block = await _logFile.WriteBlockAsync(buffer.Buffer, index);

                    totalWrites += index;
                    blocks = blocks.Add(block);
                }
                _logger
                    .AddContext("ru", ru)
                    .AddContext("cosmosPageCount", cosmosPageCount)
                    .AddContext("totalWrites", totalWrites)
                    .AddContext("blockCount", blocks.Count)
                    .WriteEvent(eventName);
                _logger.Display($"Wrote {totalWrites} bytes in {blocks.Count} blocks");
                _logger.Display($"Used {ru} RUs on {cosmosPageCount} Cosmos pages");
                _logger.WriteEvent($"{eventName}-End");

                return blocks;
            }
        }
    }
}