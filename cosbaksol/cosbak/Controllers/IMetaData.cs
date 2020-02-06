using System.IO;

namespace Cosbak.Controllers
{
    public interface IMetaData
    {
        string Id { get; }

        int IndexSize { get; }
        
        int ContentSize { get; }

        long TimeStamp { get; }

        void Write(Stream stream);
    }
}