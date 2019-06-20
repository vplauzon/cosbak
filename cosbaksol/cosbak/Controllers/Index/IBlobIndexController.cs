using System;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public interface IBlobIndexController
    {
        Task AppendAsync(Memory<byte> index);
    }
}