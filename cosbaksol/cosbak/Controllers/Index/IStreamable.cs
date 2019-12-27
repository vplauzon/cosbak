using System.IO;

namespace Cosbak.Controllers.Index
{
    public interface IStreamable
    {
        int Size { get; }

        void Write(Stream stream);
    }
}