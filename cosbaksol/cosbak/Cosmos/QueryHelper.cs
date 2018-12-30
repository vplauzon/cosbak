using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    internal static class QueryHelper
    {
        public async static Task<T[]> GetAllResultsAsync<T>(IDocumentQuery<T> query)
        {
            return await GetAllResultsAsync(query, d => d);
        }

        public async static Task<U[]> GetAllResultsAsync<T, U>(IDocumentQuery<T> query, Func<T, U> transform)
        {
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }
            if (transform == null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            var list = new List<U>();

            while (query.HasMoreResults)
            {
                var docs = await query.ExecuteNextAsync<T>();
                var transformed = from d in docs
                                  select transform(d);

                list.AddRange(transformed);
            }

            return list.ToArray();
        }
    }
}