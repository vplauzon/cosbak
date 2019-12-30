using Cosbak.Controllers.Index;
using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace cosbak.test.unit
{
    public class SortedIndexedDocumentEnumerableTest
    {
        [Fact]
        public async Task AllItems()
        {
            var indexingBuffer = new IndexingBuffer(
                20000,
                20000,
                (indexBuffer, indexSize, contentBuffer, contentSize) =>
                {
                    var enumerable = new SortedIndexedDocumentEnumerable(
                        indexBuffer,
                        indexSize,
                        contentBuffer);
                    var allItems = enumerable.AllItems.ToArray();
                    dynamic content1 = allItems[0].content;
                    dynamic content2 = allItems[1].content;
                    dynamic content3 = allItems[2].content;

                    Assert.NotEqual(content1.name, content2.name);
                    Assert.NotEqual(content2.name, content3.name);

                    return Task.CompletedTask;
                });
            var doc1 = new { name = "Joe" };
            var doc2 = new { name = "Bob" };
            var doc3 = new { name = "Arthur" };
            var buffer1 = Serialize(doc1);
            var buffer2 = Serialize(doc2);
            var buffer3 = Serialize(doc3);
            var meta1 = new DocumentMetaData("joe", null, 1, buffer1.Length);
            var meta2 = new DocumentMetaData("bob", null, 1, buffer2.Length);
            var meta3 = new DocumentMetaData("ar", null, 1, buffer3.Length);

            await indexingBuffer.WriteAsync(meta1, buffer1);
            await indexingBuffer.WriteAsync(meta2, buffer2);
            await indexingBuffer.WriteAsync(meta3, buffer3);
            await indexingBuffer.FlushAsync();
        }

        [Fact]
        public async Task GetLatestItems()
        {
            var indexingBuffer = new IndexingBuffer(
                20000,
                20000,
                (indexBuffer, indexSize, contentBuffer, contentSize) =>
                {
                    var enumerable = new SortedIndexedDocumentEnumerable(
                        indexBuffer,
                        indexSize,
                        contentBuffer);
                    var allItems = enumerable.GetLatestItems(3).ToArray();
                    dynamic content1 = allItems[0].content;
                    dynamic content2 = allItems[1].content;
                    dynamic content3 = allItems[2].content;

                    Assert.NotEqual(content1.name, content2.name);
                    Assert.NotEqual(content2.name, content3.name);

                    return Task.CompletedTask;
                });
            var doc1 = new { name = "Joe" };
            var doc2 = new { name = "Bob" };
            var doc3 = new { name = "Arthur" };
            var buffer1 = Serialize(doc1);
            var buffer2 = Serialize(doc2);
            var buffer3 = Serialize(doc3);
            var meta1 = new DocumentMetaData("joe", null, 1, buffer1.Length);
            var meta2 = new DocumentMetaData("bob", null, 2, buffer2.Length);
            var meta3 = new DocumentMetaData("ar", null, 3, buffer3.Length);

            await indexingBuffer.WriteAsync(meta1, buffer1);
            await indexingBuffer.WriteAsync(meta2, buffer2);
            await indexingBuffer.WriteAsync(meta3, buffer3);
            await indexingBuffer.FlushAsync();
        }

        private static byte[] Serialize(object obj)
        {
            var stream = new MemoryStream();
            var writer = new Utf8JsonWriter(stream);

            JsonSerializer.Serialize(writer, obj);
            writer.Flush();

            return stream.ToArray();
        }
    }
}