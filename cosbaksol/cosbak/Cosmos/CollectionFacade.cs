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

        async Task<long?> ICollectionFacade.GetLastUpdateTimeAsync(long fromTime, int maxItemCount)
        {
            var countQuery = _container.GetItemQueryIterator<long>(
                new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE c._ts > @fromTime")
                .WithParameter("@fromTime", fromTime));
            var countResults = await QueryHelper.GetAllResultsAsync(countQuery);
            var count = countResults.Content.First();

            if (count == 0)
            {
                return null;
            }
            else
            {
                var offset = Math.Max(0, count - maxItemCount);
                var lastUpdateTimeQuery = _container.GetItemQueryIterator<long>(
                    new QueryDefinition(
                        "SELECT VALUE c._ts FROM c WHERE c._ts > @fromTime "
                        + "ORDER BY c._ts DESC OFFSET @offset LIMIT 1")
                    .WithParameter("@fromTime", fromTime)
                    .WithParameter("@offset", offset));
                var timeResults = await QueryHelper.GetAllResultsAsync(lastUpdateTimeQuery);
                var time = timeResults.Content.Count == 0 ? (long?)null : timeResults.Content.First();

                _logger
                    .AddContext("ru", countResults.RequestCharge + timeResults.RequestCharge)
                    .WriteEvent("GetLastUpdateTimeAsync");

                return time;
            }
        }

        StreamIterator ICollectionFacade.GetTimeWindowDocuments(long minTime, long maxTime)
        {
            var lastUpdateTimeQuery = _container.GetItemQueryStreamIterator(
                new QueryDefinition(
                    "SELECT * FROM c WHERE c._ts > @minTime AND c._ts <= @maxTime")
                .WithParameter("@minTime", minTime)
                .WithParameter("@maxTime", maxTime));

            return new StreamIterator(lastUpdateTimeQuery);
        }
    }
}