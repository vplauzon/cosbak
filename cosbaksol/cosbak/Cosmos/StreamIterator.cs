using Microsoft.Azure.Cosmos;
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

        public async Task<Stream> ReadNextAsync()
        {
            var response = await _iterator.ReadNextAsync();

            return response.Content;
        }
    }
}