using Cosbak.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Cosbak.Controllers.Index
{
    internal class LogDocumentBatch : LogItemBatch<JsonElement>
    {
        public LogDocumentBatch(long batchTimeStamp, ReadOnlySequence<byte> sequence)
            : base(batchTimeStamp, ReadDocumentsFromSequence(sequence))
        {
        }

        private static IEnumerable<JsonElement> ReadDocumentsFromSequence(
            ReadOnlySequence<byte> sequence)
        {
            var initialReader = new Utf8JsonReader(sequence);

            //  To start object
            initialReader.Read();
            //  To property
            initialReader.Read();
            while (GetValue(initialReader) != "Documents")
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

            //  Work with index since Utf8JsonReader can't be used inside broken loops
            var index = initialReader.TokenStartIndex;
            do
            {
                var result = DeserializeElement(sequence.Slice(index));

                yield return result.element;
                index += result.offset;

                if (!result.shouldContinueLoop)
                {
                    break;
                }
            }
            while (true);
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
            var shouldContinueLoop = reader.TokenType == JsonTokenType.StartObject;

            return (element, shouldContinueLoop, reader.TokenStartIndex);
        }

        private static string GetValue(Utf8JsonReader reader)
        {
            var value = ASCIIEncoding.UTF8.GetString(reader.ValueSpan);

            return value;
        }
    }
}