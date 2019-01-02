using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cosbak.Cosmos
{
    public class DocumentObject
    {
        public DocumentObject(DocumentMetaData metaData, JObject content)
        {
            MetaData = metaData;
            Content = content;
        }

        public DocumentMetaData MetaData { get; }

        public JObject Content { get; }

        public void WriteContentAsync(Stream contentStream)
        {
            //  Replace by BsonWriter to serialize in BSON
            var writer = new JsonTextWriter(new StreamWriter(contentStream));

            Content.WriteTo(writer);
        }
    }
}