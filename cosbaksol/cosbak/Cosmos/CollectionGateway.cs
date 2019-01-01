using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

namespace Cosbak.Cosmos
{
    internal class CollectionGateway : ICollectionGateway
    {
        private readonly DocumentClient _client;
        private readonly string _collectionName;
        private readonly IDatabaseGateway _parent;
        private readonly Uri _collectionUri;

        public CollectionGateway(DocumentClient client, string collectionName, IDatabaseGateway parent)
        {
            _client = client;
            _collectionName = collectionName;
            _parent = parent;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(_parent.DatabaseName, _collectionName);
        }

        IDatabaseGateway ICollectionGateway.Parent => _parent;

        string ICollectionGateway.CollectionName => _collectionName;

        async Task<IImmutableList<IPartitionGateway>> ICollectionGateway.GetPartitionsAsync()
        {
            var keyRanges = await _client.ReadPartitionKeyRangeFeedAsync(_collectionUri);
            var gateways = from k in keyRanges
                           select new PartitionGateway(_client, this, k.Id);
            var partitions = ImmutableArray<IPartitionGateway>.Empty.AddRange(gateways);

            return partitions;
        }
    }
}