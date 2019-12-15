using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    internal class CollectionFacade : ICollectionFacade
    {
        private readonly Container _container;
        private readonly string _partitionPath;
        private readonly IDatabaseFacade _parent;
        private readonly ILogger _logger;

        public CollectionFacade(
            Container container,
            string partitionPath,
            IDatabaseFacade parent,
            ILogger logger)
        {
            _container = container;
            _partitionPath = partitionPath;
            _parent = parent;
            _logger = logger.AddContext("collection", container.Id);
        }

        IDatabaseFacade ICollectionFacade.Parent => _parent;

        string ICollectionFacade.CollectionName => _container.Id;

        string ICollectionFacade.PartitionPath => _partitionPath;

        async Task<(long currentTimeStamp, int count, long maxTimeStamp)> ICollectionFacade.SizeTimeWindowAsync(
            long minTimeStamp,
            int maxItemCount)
        {
            var countQuery =
                "SELECT GetCurrentTimestamp() AS currentTimeStamp, COUNT(1) AS count "
                + "FROM c WHERE c._ts > @minTimeStamp";
            var currentIterator = _container.GetItemQueryIterator<dynamic>(
                new QueryDefinition(countQuery)
                .WithParameter("@minTimeStamp", minTimeStamp));
            var countResult = await QueryHelper.GetAllResultsAsync(currentIterator);
            (long currentTimeStamp, int count) current = (
                //  GetCurrentTimestamp() returns in miliseconds while _ts is in second
                countResult.Content.First().currentTimeStamp / 1000,
                countResult.Content.First().count);

            if (current.count <= maxItemCount)
            {
                _logger
                    .AddContext("ru", countResult.RequestCharge)
                    .WriteEvent("SizeTimeWindowAsync");

                return (current.currentTimeStamp, current.count, current.currentTimeStamp);
            }
            else
            {
                var offset = current.count - maxItemCount;
                var maxTimeStampQuery = "SELECT VALUE c._ts FROM c WHERE c._ts > @minTimeStamp "
                    + "ORDER BY c._ts DESC OFFSET @offset LIMIT 1";
                var maxTimeStampIterator = _container.GetItemQueryIterator<long>(
                    new QueryDefinition(maxTimeStampQuery)
                    .WithParameter("@minTimeStamp", minTimeStamp)
                    .WithParameter("@offset", offset));
                var maxTimeStampResult = await QueryHelper.GetAllResultsAsync(maxTimeStampIterator);
                var maxTimeStamp = maxTimeStampResult.Content.First();

                _logger
                    .AddContext("ru", countResult.RequestCharge + maxTimeStampResult.RequestCharge)
                    .WriteEvent("SizeTimeWindowAsync");

                return (current.currentTimeStamp, current.count, maxTimeStamp);
            }
        }

        StreamIterator ICollectionFacade.GetTimeWindowDocuments(long minTimeStamp, long maxTimeStamp)
        {
            var lastUpdateTimeQuery = _container.GetItemQueryStreamIterator(
                new QueryDefinition(
                    "SELECT * FROM c WHERE c._ts > @minTimeStamp AND c._ts <= @maxTimeStamp")
                .WithParameter("@minTimeStamp", minTimeStamp)
                .WithParameter("@maxTimeStamp", maxTimeStamp));

            return new StreamIterator(lastUpdateTimeQuery);
        }

        StreamIterator ICollectionFacade.GetAllIds()
        {
            var partitionKeyDotPath = string.Join('.', _partitionPath.Split('/').Skip(1));
            var lastUpdateTimeQuery = _container.GetItemQueryStreamIterator(
                new QueryDefinition($"SELECT c.id, c.{partitionKeyDotPath} FROM c"));

            return new StreamIterator(lastUpdateTimeQuery);
        }
    }
}