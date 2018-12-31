using Microsoft.Azure.Documents.Client;

namespace Cosbak.Cosmos
{
    internal class CollectionGateway : ICollectionGateway
    {
        private readonly DocumentClient _client;
        private readonly string _collectionName;
        private readonly DatabaseGateway _parent;

        public CollectionGateway(DocumentClient client, string collectionName, DatabaseGateway parent)
        {
            _client = client;
            _collectionName = collectionName;
            _parent = parent;
        }

        IDatabaseGateway ICollectionGateway.Parent => _parent;

        string ICollectionGateway.CollectionName => _collectionName;
    }
}