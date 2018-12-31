namespace Cosbak.Cosmos
{
    public interface ICollectionGateway
    {
        IDatabaseGateway Parent { get; }

        string CollectionName { get; }
    }
}