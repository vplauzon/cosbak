using Cosbak.Storage;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Threading.Tasks;

namespace Cosbak
{
    public class Logger : ILogger
    {
        private static readonly int MAX_BUFFER_SIZE = 1 * 1024 * 1024;
        private static readonly int MAX_BLOCKS = 50000;
        private static readonly TimeSpan MAX_TIME = TimeSpan.FromSeconds(3);

        private readonly Guid _sessionId = Guid.NewGuid();
        private readonly IStorageFacade _storageFacade;
        private readonly JsonSerializer _serializer;
        private string? _blobName = null;
        private int _blocks = 0;
        private Stream _stream;
        private TextWriter _writer;
        private Task? _lastWriteTask = null;
        private DateTime? _lastWriteTime = null;

        public Logger(IStorageFacade storageFacade)
        {
            _storageFacade = storageFacade;
            _serializer = JsonSerializer.Create(new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            });
            _stream = new MemoryStream();
            _writer = new StreamWriter(_stream);
        }

        void ILogger.Display(
                string text,
                IImmutableDictionary<string, string>? context)
        {
            Console.WriteLine(text);

            PushLog("display", new { Text = text }, context);
        }

        void ILogger.DisplayError(
            Exception exception,
            IImmutableDictionary<string, string>? context)
        {
            Console.Error.WriteLine($"Exception:  '{exception.GetType().Name}'");
            Console.Error.WriteLine($"Full Name:  '{exception.GetType().FullName}'");
            Console.Error.WriteLine($"Stack Trace:  '{exception.StackTrace}'");

            PushLog("error", new
            {
                Exception = exception.GetType().FullName,
                exception.StackTrace
            }, context);
        }

        void ILogger.WriteEvent(
            string eventName,
            IImmutableDictionary<string, string>? context,
            double? metric,
            long? count,
            TimeSpan? duration)
        {
            PushLog(
                "event",
                new
                {
                    eventName,
                    metric,
                    count,
                    duration
                },
                context);
        }

        async Task ILogger.FlushAsync()
        {
            await FlushAsync();
        }

        private void PushLog(
            string eventType,
            object content,
            IImmutableDictionary<string, string>? context)
        {
            var telemetry = new
            {
                SessionId = _sessionId,
                eventType,
                content,
                context,
                TimeStamp = DateTime.Now.ToUniversalTime()
            };
            var now = DateTime.Now;

            lock (_stream)
            {
                _serializer.Serialize(_writer, telemetry);
                _writer.Flush();
                if (_stream.Length > MAX_BUFFER_SIZE
                    || (_lastWriteTime != null && now.Subtract(_lastWriteTime.Value) > MAX_TIME))
                {
                    _lastWriteTask = FlushAsync();
                }
            }
        }

        private async Task FlushAsync()
        {
            //  Buffer the last write task
            var lastWriteTask = _lastWriteTask;
            //  Buffer the stream
            var stream = _stream;

            //  Flip the streams and companions
            lock (_stream)
            {
                if (_stream.Length == 0)
                {
                    return;
                }
                else
                {
                    _stream = new MemoryStream();
                    _writer = new StreamWriter(_stream);
                    _lastWriteTime = null;
                }
            }
            if (lastWriteTask != null)
            {   //  First wait for the last write
                await lastWriteTask;
            }
            stream.Position = 0;
            if (_blobName == null)
            {
                _blobName = DateTime
                    .Now
                    .ToString()
                    .Replace(' ', '_')
                    .Replace(':', '-')
                    .Replace('/', '-')
                    + ".json";
                await _storageFacade.CreateAppendBlobAsync(_blobName);
            }
            await _storageFacade.AppendBlobAsync(_blobName, stream);
            ++_blocks;
            if (_blocks == MAX_BLOCKS)
            {
                _blobName = null;
            }
        }
    }
}