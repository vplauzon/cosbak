﻿namespace Cosbak.Controllers.Backup
{
    public interface ICosmosCollectionController
    {
        string Account { get; }

        string Database { get; }

        string Collection { get; }
    }
}