﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Cosbak.Config;
using Cosbak.Cosmos;
using Cosbak.Storage;

namespace Cosbak
{
    public class BackupController
    {
        private const int DEFAULT_RAM = 20;

        private readonly IImmutableList<ICosmosDbGateway> _cosmosDbGateways;
        private readonly IStorageGateway _storageGateway;
        private readonly int _ram;

        public BackupController(
            IEnumerable<ICosmosDbGateway> cosmosDbGateways,
            IStorageGateway storageGateway,
            int? ram)
        {
            if (cosmosDbGateways == null)
            {
                throw new ArgumentNullException(nameof(cosmosDbGateways));
            }
            _cosmosDbGateways = ImmutableArray<ICosmosDbGateway>.Empty.AddRange(cosmosDbGateways);
            _storageGateway = storageGateway ?? throw new ArgumentNullException(nameof(storageGateway));
            _ram = ram == null
                ? DEFAULT_RAM
                : ram.Value;
        }

        public async Task BackupAsync()
        {
            foreach (var cosmosAccount in _cosmosDbGateways)
            {
                foreach (var db in await cosmosAccount.GetDatabasesAsync())
                {
                    foreach (var collection in db.GetCollectionsAsync())
                    {
                        await BackupCollectionAsync(collection);
                    }
                }
            }
        }

        private async Task BackupCollectionAsync(IDocCollection collection)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}