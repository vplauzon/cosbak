namespace Cosbak.Controllers.Index
{
    public interface IIndexCollectionBackupController
    {
        string Account { get; }

        string Database { get; }

        string Collection { get; }
    }
}