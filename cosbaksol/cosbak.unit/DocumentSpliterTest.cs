using Cosbak;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace cosbak.unit
{
    public class DocumentSpliterTest
    {
        [Fact]
        public void Rematerialize()
        {
            var original = new JObject();
            var partitionPathParts = new[] { "company" };

            original.Add("id", "1234");
            original.Add("_ts", 874523);
            original.Add("name", "John");
            original.Add("age", 42);
            original.Add("company", "Microsoft");

            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                var meta = DocumentSpliter.Write(original, partitionPathParts, writer);

                stream.Flush();
                stream.Position = 0;

                using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
                {
                    var clone = DocumentSpliter.Read(meta, partitionPathParts, reader);

                    Assert.NotNull(clone);
                    Assert.Equal(5 - 1, clone.Count);
                    Assert.Equal(original["id"].Value<string>(), clone["id"].Value<string>());
                    Assert.Equal(
                        original["name"].Value<string>(), clone["name"].Value<string>());
                    Assert.Equal(original["age"].Value<int>(), clone["age"].Value<int>());
                    Assert.Equal(
                        original["company"].Value<string>(), clone["company"].Value<string>());
                }
            }
        }


        [Fact]
        public void DeepPartitionKey()
        {
            var original = new JObject();
            var info = new JObject();
            var address = new JObject();
            var partitionPathParts = new[] { "address/street" };

            original.Add("id", "1234");
            original.Add("_ts", 874523);
            info.Add("name", "John");
            info.Add("age", 42);
            original.Add("info", info);
            address.Add("street", "Hayes");
            address.Add("number", "4A");
            original.Add("address", address);
            original.Add("company", "Microsoft");

            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                var meta = DocumentSpliter.Write(original, partitionPathParts, writer);

                stream.Flush();
                stream.Position = 0;

                using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
                {
                    var clone = DocumentSpliter.Read(meta, partitionPathParts, reader);

                    Assert.NotNull(clone);
                    Assert.Equal(5 - 1, clone.Count);
                    Assert.Equal(original["id"].Value<string>(), clone["id"].Value<string>());
                    Assert.Equal(
                        original["info"]["name"].Value<string>(),
                        clone["info"]["name"].Value<string>());
                    Assert.Equal(
                        original["info"]["age"].Value<int>(),
                        clone["info"]["age"].Value<int>());
                    Assert.Equal(
                        original["address"]["street"].Value<string>(),
                        clone["address"]["street"].Value<string>());
                    Assert.Equal(
                        original["address"]["number"].Value<string>(),
                        clone["address"]["number"].Value<string>());
                    Assert.Equal(
                        original["company"].Value<string>(), clone["company"].Value<string>());
                }
            }
        }
    }
}