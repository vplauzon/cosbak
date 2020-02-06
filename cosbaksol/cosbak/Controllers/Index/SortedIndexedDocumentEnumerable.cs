using Cosbak.Controllers.Index;
using Microsoft.VisualBasic.CompilerServices;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Reflection.Metadata;
using System.Text;
using System.Text.Json;

namespace Cosbak.Controllers.Index
{
    internal class SortedIndexedDocumentEnumerable :
        SortedIndexedContentEnumerable<DocumentMetaData>
    {
        public SortedIndexedDocumentEnumerable(
            byte[] indexBuffer,
            int indexBufferLength,
            byte[] contentBuffer) : base(indexBuffer,
                indexBufferLength,
                contentBuffer,
                stream => DocumentMetaData.Read(stream),
                Compare)
        {
        }

        private static int Compare(DocumentMetaData x, DocumentMetaData y)
        {
            //  Compare partition hash, then ids, then timestamp
            if (x.PartitionHash != y.PartitionHash)
            {
                return x.PartitionHash.CompareTo(y.PartitionHash);
            }
            else
            {
                IMetaData metaX = x;
                IMetaData metaY = y;

                if (metaX.Id != metaY.Id)
                {
                    return metaX.Id.CompareTo(metaY.Id);
                }
                else
                {
                    return metaX.TimeStamp.CompareTo(metaY.TimeStamp);
                }
            }
        }
    }
}