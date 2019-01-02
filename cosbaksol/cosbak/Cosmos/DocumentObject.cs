using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cosbak.Cosmos
{
    public class DocumentObject
    {
        public DocumentObject(DocumentMetaData metaData, IDictionary<string, object> content)
        {
            MetaData = metaData;
            Content = content;
        }

        public DocumentMetaData MetaData { get; }

        public IDictionary<string, object> Content { get; }

        public void WriteContentAsync(Stream contentStream)
        {
            var json = JsonConvert.SerializeObject(Content);
            var writer = new StreamWriter(contentStream);

            writer.Write(json);
            writer.Flush();
        }
    }
}