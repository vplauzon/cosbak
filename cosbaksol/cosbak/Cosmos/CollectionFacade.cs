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

        public CollectionFacade(Container container, string partitionPath, IDatabaseFacade parent)
        {
            _container = container;
            _partitionPath = partitionPath;
            _parent = parent;
        }

        IDatabaseFacade ICollectionFacade.Parent => _parent;

        string ICollectionFacade.CollectionName => _container.Id;

        string ICollectionFacade.PartitionPath => _partitionPath;

        async Task<long?> ICollectionFacade.GetLastUpdateTimeAsync(long fromTime, int maxItemCount)
        {
            //var countSql = new SqlQuerySpec(
            //    "SELECT VALUE COUNT(1) FROM c WHERE c._ts > @fromTime",
            //    new SqlParameterCollection(
            //        new[] {
            //            new SqlParameter("@fromTime", fromTime)
            //        }));
            //var countQuery = _container.CreateDocumentQuery<long>(
            //    _collectionUri,
            //    countSql,
            //    new FeedOptions
            //    {
            //        EnableCrossPartitionQuery = true
            //    });
            //var countResults = await QueryHelper.GetAllResultsAsync(countQuery.AsDocumentQuery());
            //var count = countResults.First();

            //if (count == 0)
            //{
            //    return null;
            //}
            //else
            //{
            //    var lastUpdateTimeSql = new SqlQuerySpec(
            //        "SELECT c._ts FROM c WHERE c._ts > @fromTime ORDER BY c._ts DESC OFFSET @offset LIMIT 1",
            //        new SqlParameterCollection(
            //            new[]
            //            {
            //                new SqlParameter("@fromTime", fromTime),
            //                new SqlParameter("@offset", Math.Max(0, count- maxItemCount))
            //            }));
            //    var timeQuery = _container.CreateDocumentQuery<IDictionary<string, long>>(
            //        _collectionUri,
            //        lastUpdateTimeSql,
            //        new FeedOptions
            //        {
            //            EnableCrossPartitionQuery = true
            //        });
            //    var timeResults = await QueryHelper.GetAllResultsAsync(timeQuery.AsDocumentQuery());
            //    var time = timeResults.Length == 0 ? (long?)null : timeResults[0].First().Value;

            //    return time;
            //}
            await Task.FromResult(42);
            throw new NotImplementedException();
        }
    }
}