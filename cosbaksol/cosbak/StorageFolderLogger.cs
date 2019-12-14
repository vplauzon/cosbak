using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Net.NetworkInformation;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cosbak
{
    public class StorageFolderLogger : ILogger
    {
        #region Inner types
        private class DerivedLogger : ILogger
        {
            private readonly StorageFolderLogger _parentLogger;
            private readonly IImmutableDictionary<string, string> _context;

            public DerivedLogger(StorageFolderLogger parentLogger, IImmutableDictionary<string, string> baseContext)
            {
                _parentLogger = parentLogger;
                _context = baseContext;
            }

            ILogger ILogger.AddContext<T>(string label, T value)
            {
                if (value != null)
                {
                    var textValue = value.ToString();

                    if (textValue != null)
                    {
                        return new DerivedLogger(_parentLogger, _context.Add(label, textValue));
                    }
                }

                return this;
            }

            void ILogger.Display(string text)
            {
                _parentLogger.Display(text, _context);
            }

            void ILogger.DisplayError(Exception exception)
            {
                _parentLogger.DisplayError(exception, _context);
            }

            void ILogger.WriteEvent(string eventName)
            {
                _parentLogger.WriteEvent(eventName, _context);
            }

            Task ILogger.FlushAsync()
            {
                return _parentLogger.FlushAsync();
            }
        }
        #endregion

        private static readonly int MAX_BUFFER_SIZE = 1 * 1024 * 1024;
        private static readonly int MAX_BLOCKS = 50000;
        private static readonly TimeSpan MAX_TIME = TimeSpan.FromSeconds(3);

        private readonly Guid _sessionId = Guid.NewGuid();
        private readonly IStorageFacade _storageFacade;
        private readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions
        {
            IgnoreNullValues = true
        };
        private string? _blobName = null;
        private int _blocks = 0;
        private Stream _stream;
        private Task? _lastWriteTask = null;
        private DateTime? _lastWriteTime = null;

        public StorageFolderLogger(IStorageFacade storageFacade)
        {
            _storageFacade = storageFacade;
            _stream = new MemoryStream();
        }

        ILogger ILogger.AddContext<T>(string label, T value)
        {
            if (value != null)
            {
                var textValue = value.ToString();

                if (textValue != null)
                {
                    return new DerivedLogger(
                        this,
                        ImmutableDictionary<string, string>.Empty.Add(label, textValue));
                }
            }

            return this;
        }

        void ILogger.Display(string text)
        {
            Display(text, ImmutableDictionary<string, string>.Empty);
        }

        void ILogger.DisplayError(Exception exception)
        {
            DisplayError(exception, ImmutableDictionary<string, string>.Empty);
        }

        void ILogger.WriteEvent(string eventName)
        {
            WriteEvent(eventName, ImmutableDictionary<string, string>.Empty);
        }

        async Task ILogger.FlushAsync()
        {
            await FlushAsync();
        }

        private void Display(string text, IImmutableDictionary<string, string> context)
        {
            Console.WriteLine(text);

            PushLog("display", new { Text = text }, context);
        }

        private void DisplayError(Exception exception, IImmutableDictionary<string, string> context)
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

        private void WriteEvent(string eventName, IImmutableDictionary<string, string> context)
        {
            PushLog(
                "event",
                new
                {
                    eventName
                },
                context);
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
                JsonSerializer.SerializeAsync(_stream, telemetry, _serializerOptions);
                _stream.WriteByte((byte)'\n');
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

            //  Flip the streams
            lock (_stream)
            {
                if (_stream.Length == 0)
                {
                    return;
                }
                else
                {
                    _stream = new MemoryStream();
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