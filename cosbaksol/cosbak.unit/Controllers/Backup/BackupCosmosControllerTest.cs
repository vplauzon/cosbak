using Cosbak.Controllers.Backup;
using Cosbak.Cosmos;
using Cosbak.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace cosbak.unit.Controllers.Backup
{
    public class BackupCosmosControllerTest
    {
        [Fact]
        public async Task All()
        {
            var controller = new BackupCosmosController(
                CreateMockDatabaseAccountFacade(),
                CreateMockLogger(),
                null) as IBackupCosmosController;
            var collections = await controller.GetCollectionsAsync();

            Assert.NotNull(collections);

            var sorted = (from c in collections
                          orderby c.Database, c.Collection
                          select c).ToArray();

            Assert.Equal(5, sorted.Length);
            Assert.Equal("db1", sorted[0].Database);
            Assert.Equal("db1", sorted[1].Database);
            Assert.Equal("db2", sorted[2].Database);
            Assert.Equal("db2", sorted[3].Database);
            Assert.Equal("db2", sorted[4].Database);

            Assert.Equal("a", sorted[0].Collection);
            Assert.Equal("b", sorted[1].Collection);
            Assert.Equal("x", sorted[2].Collection);
            Assert.Equal("y", sorted[3].Collection);
            Assert.Equal("z", sorted[4].Collection);
        }

        [Fact]
        public async Task NonExistingCollection()
        {
            var controller = new BackupCosmosController(
                CreateMockDatabaseAccountFacade(),
                CreateMockLogger(),
                new[] { "db1.c" }) as IBackupCosmosController;
            var collections = await controller.GetCollectionsAsync();

            Assert.NotNull(collections);
            Assert.False(collections.Any());
        }

        [Fact]
        public async Task OneCollection()
        {
            var controller = new BackupCosmosController(
                CreateMockDatabaseAccountFacade(),
                CreateMockLogger(),
                new[] { "db1.a" }) as IBackupCosmosController;
            var collections = await controller.GetCollectionsAsync();

            Assert.NotNull(collections);

            var sorted = (from c in collections
                          orderby c.Database, c.Collection
                          select c).ToArray();

            Assert.True(1 == sorted.Length);
            Assert.Equal("db1", sorted[0].Database);
            Assert.Equal("a", sorted[0].Collection);
        }

        private static IDatabaseAccountFacade CreateMockDatabaseAccountFacade()
        {
            var account = new Mock<IDatabaseAccountFacade>();
            var db1 = new Mock<IDatabaseFacade>();
            var db2 = new Mock<IDatabaseFacade>();
            var collectionA = new Mock<ICollectionFacade>();
            var collectionB = new Mock<ICollectionFacade>();
            var collectionX = new Mock<ICollectionFacade>();
            var collectionY = new Mock<ICollectionFacade>();
            var collectionZ = new Mock<ICollectionFacade>();

            collectionA
                .Setup(c => c.CollectionName)
                .Returns("a");
            collectionA
                .Setup(c => c.Parent)
                .Returns(db1.Object);
            collectionB
                .Setup(c => c.CollectionName)
                .Returns("b");
            collectionB
                .Setup(c => c.Parent)
                .Returns(db1.Object);
            collectionX
                .Setup(c => c.CollectionName)
                .Returns("x");
            collectionX
                .Setup(c => c.Parent)
                .Returns(db2.Object);
            collectionY
                .Setup(c => c.CollectionName)
                .Returns("y");
            collectionY
                .Setup(c => c.Parent)
                .Returns(db2.Object);
            collectionZ
                .Setup(c => c.CollectionName)
                .Returns("z");
            collectionZ
                .Setup(c => c.Parent)
                .Returns(db2.Object);

            db1
                .Setup(db => db.DatabaseName)
                .Returns("db1");
            db1
                .Setup(db => db.GetCollectionsAsync())
                .ReturnsAsync(() => new[] { collectionA.Object, collectionB.Object });
            db2
                .Setup(db => db.DatabaseName)
                .Returns("db2");
            db2
                .Setup(db => db.GetCollectionsAsync())
                .ReturnsAsync(() => new[]
                {
                    collectionX.Object,
                    collectionY.Object,
                    collectionZ.Object
                });

            account
                .Setup(a => a.GetDatabasesAsync())
                .ReturnsAsync(() => new[] { db1.Object, db2.Object });

            return account.Object;
        }

        private static ILogger CreateMockLogger()
        {
            var loggerMock = new Mock<ILogger>();

            return loggerMock.Object;
        }
    }
}