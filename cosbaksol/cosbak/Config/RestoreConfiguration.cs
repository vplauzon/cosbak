using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Cosbak.Config
{
    public class RestoreConfiguration
    {
        public CosmosAccountConfiguration CosmosAccount { get; set; } = new CosmosAccountConfiguration();

        public StorageAccountConfiguration StorageAccount { get; set; } = new StorageAccountConfiguration();
        
        public CompleteCollectionConfiguration SourceCollection { get; set; } = new CompleteCollectionConfiguration();
        
        public CollectionConfiguration TargetCollection { get; set; } = new CollectionConfiguration();

        public TechnicalConstants Constants { get; set; } = new TechnicalConstants();

        public void Validate()
        {
            CosmosAccount.Validate();
            StorageAccount.Validate();
            SourceCollection.Validate();
            TargetCollection.Validate();
        }
    }
}