using System;

namespace cosbak.test.feature
{
    public class CosmosFixture : IDisposable
    {
        void IDisposable.Dispose()
        {
            CosmosCollectionRental.DeleteDatabaseAsync().Wait();
        }
    }
}