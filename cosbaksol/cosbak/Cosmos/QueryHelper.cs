using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    internal static class QueryHelper
    {
        public async static Task<QueryResult<T>> GetAllResultsAsync<T>(FeedIterator<T> query)
        {
            return await GetAllResultsAsync(query, d => d);
        }

        public async static Task<QueryResult<U>> GetAllResultsAsync<T, U>(FeedIterator<T> query, Func<T, U> transform)
        {
            var list = ImmutableList<U>.Empty;
            double requestCharge = 0;

            while (query.HasMoreResults)
            {
                var response = await query.ReadNextAsync();
                var transformed = from d in response
                                  select transform(d);

                requestCharge += response.RequestCharge;
                list = list.AddRange(transformed);
            }

            return new QueryResult<U>(list, requestCharge);
        }
    }
}