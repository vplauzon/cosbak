using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace cosbak.test.feature
{
    internal static class CollectionComparer
    {
        public async static Task CompareDocumentCountAsync(
            Container sourceContainer,
            Container targetContainer)
        {
            var query = "SELECT VALUE COUNT(1) FROM c";
            var sourceCount = await QueryScalarAsync<long>(sourceContainer, query);
            var targetCount = await QueryScalarAsync<long>(targetContainer, query);

            Assert.Equal(sourceCount, targetCount);
        }

        private async static Task<T> QueryScalarAsync<T>(Container container, string query)
        {
            var sourceIterator = container.GetItemQueryIterator<T>(query);

            while (sourceIterator.HasMoreResults)
            {
                var results = await sourceIterator.ReadNextAsync();

                if (results.Any())
                {
                    return results.First();
                }
            }

            throw new InvalidOperationException($"No result were returned for query '{query}'");
        }
    }
}