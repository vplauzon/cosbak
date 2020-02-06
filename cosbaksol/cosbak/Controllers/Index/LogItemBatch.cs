using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace Cosbak.Controllers.Index
{
    internal abstract class LogItemBatch<ITEM>
    {
        protected LogItemBatch(long batchTimeStamp, ReadOnlySequence<byte> sequence)
        {
            BatchTimeStamp = batchTimeStamp;
            Items = ReadItemsFromSequence(sequence);
        }

        public long BatchTimeStamp { get; }

        public IEnumerable<ITEM> Items { get; }

        protected abstract string ItemProperty { get; }

        protected abstract ITEM TransformElement(JsonElement element);

        private IEnumerable<ITEM> ReadItemsFromSequence(
            ReadOnlySequence<byte> sequence)
        {
            var itemProperty = ItemProperty;
            var initialReader = new Utf8JsonReader(sequence);

            //  To start object
            initialReader.Read();
            //  To property
            initialReader.Read();
            while (GetValue(initialReader) != itemProperty)
            {
                //  To property value
                initialReader.Read();
                //  To next item
                initialReader.Read();
            }
            //  To Property value
            initialReader.Read();
            if (initialReader.TokenType != JsonTokenType.StartArray)
            {
                throw new InvalidOperationException(
                    $"Should be a start array in the JSON instead of '{initialReader.TokenType}'");
            }
            //  To first element in the array
            initialReader.Read();

            if (initialReader.TokenType != JsonTokenType.EndArray)
            {
                //  Work with index since Utf8JsonReader can't be used inside broken loops
                var index = initialReader.TokenStartIndex;
                do
                {
                    var result = DeserializeElement(sequence.Slice(index));
                    var item = TransformElement(result.element);

                    yield return item;
                    index += result.offset;

                    if (!result.shouldContinueLoop)
                    {
                        break;
                    }
                }
                while (true);
            }
        }

        private static (
            JsonElement element,
            bool shouldContinueLoop,
            long offset) DeserializeElement(ReadOnlySequence<byte> sequence)
        {
            //  This whole intricate routine exists because Utf8JsonReader can't exist
            //  inside a broken loop (i.e. a loop with yields)
            var reader = new Utf8JsonReader(sequence);
            var element = JsonSerializer.Deserialize<JsonElement>(ref reader);
            var offset = reader.TokenStartIndex;

            do
            {
                var character = (char)sequence.FirstSpan[(int)++offset];

                if (character == '{')
                {
                    return (element, true, offset);
                }
                else if (character == ']')
                {
                    return (element, false, offset);
                }
            }
            while (true);

        }

        private static string GetValue(Utf8JsonReader reader)
        {
            var value = ASCIIEncoding.UTF8.GetString(reader.ValueSpan);

            return value;
        }
    }
}