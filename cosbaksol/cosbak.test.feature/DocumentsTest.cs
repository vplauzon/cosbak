using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Xunit;

namespace cosbak.test.feature
{
    [Collection("Cosmos collection")]
    public class DocumentsTest
    {
        [Fact]
        public async Task EmptyBackup()
        {
            var container = await CosmosCollectionRental.GetCollectionAsync("empty");

        }
    }
}