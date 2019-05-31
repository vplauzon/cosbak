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
    }
}