using Microsoft.Azure.Documents.Client;

namespace Cosbak.Cosmos
{
    public class PartitionGateway : IPartitionGateway
    {
        private readonly DocumentClient _client;
        private readonly ICollectionGateway _parent;
        private readonly string _partitionKeyRangeId;

        public PartitionGateway(DocumentClient client, ICollectionGateway parent, string partitionKeyRangeId)
        {
            _client = client;
            _parent = parent;
            _partitionKeyRangeId = partitionKeyRangeId;
        }
    }
}