using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Controllers
{
    public class BufferPool : IDisposable
    {
        public static BufferPool Rent(int length)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(length);

            return new BufferPool(buffer);
        }

        private BufferPool(byte[] buffer)
        {
            Buffer = buffer;
        }

        public byte[] Buffer { get; }

        void IDisposable.Dispose()
        {
            ArrayPool<byte>.Shared.Return(Buffer);
        }
    }
}
