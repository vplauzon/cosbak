using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace cosbak.test.feature
{
    [CollectionDefinition("Cosmos collection")]
    public class CosmosCollection : ICollectionFixture<CosmosFixture>
    {
    }
}