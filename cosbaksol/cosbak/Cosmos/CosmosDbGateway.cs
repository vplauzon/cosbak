using Cosbak.Config;

namespace Cosbak.Cosmos
{
    internal class CosmosDbGateway : ICosmosDbGateway
    {
        private AccountDescription a;

        public CosmosDbGateway(AccountDescription a)
        {
            this.a = a;
        }
    }
}