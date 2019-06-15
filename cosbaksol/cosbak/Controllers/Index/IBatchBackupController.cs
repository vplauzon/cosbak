namespace Cosbak.Controllers.Index
{
    public interface IBatchBackupController
    {
        int FolderId { get; }

        long TimeStamp { get; }
    }
}