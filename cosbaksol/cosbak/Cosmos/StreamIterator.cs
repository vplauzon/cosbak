using Microsoft.Azure.Cosmos;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Cosbak.Cosmos
{
    public class StreamIterator
    {
        private readonly FeedIterator _iterator;

        public StreamIterator(FeedIterator iterator)
        {
            _iterator = iterator;
        }

        public bool HasMoreResults
        {
            get { return _iterator.HasMoreResults; }
        }

        public async Task<QueryStreamResult> ReadNextAsync()
        {
            var response = await _iterator.ReadNextAsync();

            if (response.Content == null)
            {
                throw new InvalidOperationException("No stream:  error in the query");
            }

            return new QueryStreamResult(
                response.Content,
                response.Headers.RequestCharge);
        }
    }
}