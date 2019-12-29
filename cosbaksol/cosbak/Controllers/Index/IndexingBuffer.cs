using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers.Index
{
    public class IndexingBuffer : IDisposable
    {
        private readonly BufferPool _indexBuffer;
        private readonly BufferPool _contentBuffer;
        private readonly MemoryStream _indexStream;
        private readonly MemoryStream _contentStream;
        private readonly Func<byte[], int, byte[], int, Task> _flushBuffer;

        public IndexingBuffer(
            int indexSize,
            int contentSize,
            Func<byte[], int, byte[], int, Task> flushBuffer)
        {
            _indexBuffer = BufferPool.Rent(indexSize);
            _contentBuffer = BufferPool.Rent(contentSize);
            _indexStream = new MemoryStream(_indexBuffer.Buffer);
            _contentStream = new MemoryStream(_contentBuffer.Buffer);
            _flushBuffer = flushBuffer;
        }

        void IDisposable.Dispose()
        {
            ((IDisposable)_indexBuffer).Dispose();
            ((IDisposable)_contentBuffer).Dispose();
        }

        public int ItemCount { get; private set; }

        public async Task WriteAsync(IMetaData metaData, byte[] content)
        {
            var indexSpace = _indexStream.Length - _indexStream.Position;
            var contentSpace = _contentStream.Length - _contentStream.Position;
            var hasCapacity = indexSpace >= metaData.IndexSize
                && contentSpace >= content.Length;

            if(!hasCapacity)
            {
                await FlushAsync();
            }
            metaData.Write(_indexStream);
            _contentStream.Write(content);
            ++ItemCount;
        }

        public async Task FlushAsync()
        {
            await _flushBuffer(
                _indexBuffer.Buffer,
                (int)_indexStream.Position,
                _contentBuffer.Buffer,
                (int)_contentStream.Position);
        }
    }
}