﻿using System.IO;
using System.Threading.Tasks;

namespace Cosbak.Storage
{
    public interface IStorageGateway
    {
        Task CreateBlobAsync(string appendBlobPath);

        Task AppendBlobAsync(string contentPath, Stream contentStream);
    }
}