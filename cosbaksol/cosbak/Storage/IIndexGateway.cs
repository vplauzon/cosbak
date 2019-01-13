using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Storage
{
    public interface IIndexGateway
    {
        Task PrepareAppendAsync(IEnumerable<DocumentMetaData> documentsToInsert);

        Task AppendAsync(IEnumerable<DocumentMetaData> metaData, byte[] content);

        Task ReleaseAsync();
    }
}