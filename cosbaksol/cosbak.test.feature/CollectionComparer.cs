using Microsoft.Azure.Cosmos;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
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

        public async static Task CompareDocumentsAsync(
            Container sourceContainer,
            Container targetContainer)
        {
            await CompareDocumentCountAsync(sourceContainer, targetContainer);

            var query = "SELECT * FROM c ORDER BY c.id";
            var sourceItems = QueryCollectionAsync(sourceContainer, query).GetAsyncEnumerator();
            var targetItems = QueryCollectionAsync(targetContainer, query).GetAsyncEnumerator();

            while (await sourceItems.MoveNextAsync())
            {
                await targetItems.MoveNextAsync();

                var sourceItem = sourceItems.Current;
                var targetItem = targetItems.Current;

                CompareItems(sourceItem, targetItem);
            }
        }

        private static void CompareItems(
            IDictionary<string, object> sourceItem,
            IDictionary<string, object> targetItem)
        {
            Assert.Equal(sourceItem.Count, targetItem.Count);

            foreach (var key in sourceItem.Keys)
            {
                Assert.True(targetItem.ContainsKey(key));
                if (!key.StartsWith("_"))
                {
                    CompareValue(sourceItem[key], targetItem[key]);
                }
            }
        }

        private static void CompareValue(object obj1, object obj2)
        {
            Assert.NotNull(obj1);
            Assert.NotNull(obj2);
            Assert.Equal(obj1.GetType(), obj2.GetType());
            if (obj1.GetType().IsValueType || obj1.GetType() == typeof(string))
            {
                Assert.Equal(obj1, obj2);
            }
            else
            {
                var sourceItem = ProjectToDictionary(obj1);
                var targetItem = ProjectToDictionary(obj2);

                Assert.NotNull(sourceItem);
                Assert.NotNull(targetItem);

                if (sourceItem == null || targetItem == null)
                {
                    throw new InvalidCastException("Impossible after asserts");
                }

                CompareItems(sourceItem, targetItem);
            }
        }

        private static IDictionary<string, object> ProjectToDictionary(object obj)
        {
            var serializer = new JsonSerializer();
            var writer = new StringWriter();

            serializer.Serialize(writer, obj);

            var reader = new StringReader(writer.GetStringBuilder().ToString());
            var dictionary = serializer.Deserialize(reader, typeof(IDictionary<string, object>));

            return (IDictionary<string, object>)dictionary;
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

        private async static IAsyncEnumerable<IDictionary<string, object>> QueryCollectionAsync(
            Container container,
            string query)
        {
            var sourceIterator =
                container.GetItemQueryIterator<IDictionary<string, object>>(query);

            while (sourceIterator.HasMoreResults)
            {
                var results = await sourceIterator.ReadNextAsync();

                foreach (var r in results)
                {
                    yield return r;
                }
            }
        }
    }
}