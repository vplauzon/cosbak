﻿using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace Cosbak.Cosmos
{
    public interface IPartitionFacade
    {
        ICollectionFacade Parent { get; }

        string KeyRangeId { get; }

        IAsyncStream<JObject> GetChangeFeed();
    }
}