using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    internal static class QueryHelper
    {
        public async static Task<T[]> GetAllResultsAsync<T>(FeedIterator<T> query)
        {
            return await GetAllResultsAsync(query, d => d);
        }

        public async static Task<U[]> GetAllResultsAsync<T, U>(FeedIterator<T> query, Func<T, U> transform)
        {
            var list = new List<U>();

            while (query.HasMoreResults)
            {
                var docs = await query.ReadNextAsync();
                var transformed = from d in docs
                                  select transform(d);

                list.AddRange(transformed);
            }

            return list.ToArray();
        }
    }
}