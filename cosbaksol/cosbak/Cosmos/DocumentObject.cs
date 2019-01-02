using System;
using System.Collections.Generic;
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
    }
}