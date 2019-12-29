using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Cosbak.Controllers.Index
{
    internal class SortedIndexedContentEnumerable<INDEX>
        where INDEX : IMetaData
    {
        #region Inner Types
        private struct Offset
        {
            public int IndexOffset { get; set; }

            public int ContentOffset { get; set; }
        }

        private class OffsetComparer : IComparer<int>
        {
            private readonly Func<int, int, int> _comparer;

            public OffsetComparer(Func<int, int, int> comparer)
            {
                _comparer = comparer;
            }

            int IComparer<int>.Compare(int x, int y)
            {
                return _comparer(x, y);
            }
        }
        #endregion

        private readonly ImmutableArray<Offset> _sortedOffsets;
        private readonly Func<Stream, INDEX> _indexDeserializer;
        private readonly Stream _indexStream;
        private readonly ReadOnlyMemory<byte> _contentBuffer;

        public SortedIndexedContentEnumerable(
            byte[] indexBuffer,
            int indexBufferLength,
            byte[] contentBuffer,
            Func<Stream, INDEX> indexDeserializer,
            Func<INDEX, INDEX, int> comparer)
        {
            _indexStream = new MemoryStream(indexBuffer);
            _contentBuffer = new ReadOnlyMemory<byte>(contentBuffer);

            var offsets = new List<Offset>();
            var offset = new Offset();

            //  First loop to populate the offsets
            while (_indexStream.Position < indexBufferLength)
            {
                var metadata = indexDeserializer(_indexStream);

                offsets.Add(offset);
                if (offset.IndexOffset + metadata.IndexSize != _indexStream.Position)
                {
                    throw new InvalidDataException("Index offset is inconsistant");
                }
                offset.IndexOffset = (int)_indexStream.Position;
                offset.ContentOffset += metadata.ContentSize;
            }
            //  Second loop to order the offsets
            offsets.OrderBy(o => o.IndexOffset, new OffsetComparer((x, y) =>
            {
                _indexStream.Position = x;

                var metaX = indexDeserializer(_indexStream);

                _indexStream.Position = y;

                var metaY = indexDeserializer(_indexStream);

                return comparer(metaX, metaY);
            }));

            _sortedOffsets = offsets.ToImmutableArray();
            _indexDeserializer = indexDeserializer;
        }

        public IEnumerable<(INDEX index, ReadOnlyMemory<byte> content)> AllItems
        {
            get
            {
                foreach (var offset in _sortedOffsets)
                {
                    _indexStream.Position = offset.IndexOffset;

                    var meta = _indexDeserializer(_indexStream);
                    var contentBuffer = _contentBuffer.Slice(offset.ContentOffset, meta.IndexSize);

                    yield return (meta, contentBuffer);
                }
            }
        }

        public IEnumerable<(
            string id,
            IEnumerable<(
                INDEX index,
                ReadOnlyMemory<byte> content)> items)> GetAllItemsById()
        {
            var currentId = (string?)null;
            var items = ImmutableList<(INDEX index, ReadOnlyMemory<byte> content)>.Empty;

            foreach (var item in AllItems)
            {
                if (currentId == null || item.index.Id == currentId)
                {
                    currentId = item.index.Id;
                    items = items.Add(item);
                }
                else
                {
                    yield return (currentId, items.ToImmutableArray());
                }
            }

            if (currentId != null)
            {
                yield return (currentId, items.ToImmutableArray());
            }
        }

        public IEnumerable<(INDEX index, ReadOnlyMemory<byte> content)> GetLatestItems(
            long upToTimeStamp)
        {
            foreach (var grouping in GetAllItemsById())
            {
                var truncateItems = from i in grouping.items
                                    where i.index.TimeStamp <= upToTimeStamp
                                    select i;

                if (truncateItems.Any())
                {
                    yield return truncateItems.Last();
                }
            }
        }
    }
}