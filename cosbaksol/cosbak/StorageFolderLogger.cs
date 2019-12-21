using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.Concurrent;
using System.IO;
using System.Net.NetworkInformation;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;

namespace Cosbak
{
    internal class StorageFolderLogger : ILogger
    {
        #region Inner types
        private class DerivedLogger : ILogger
        {
            private readonly StorageFolderLogger _parentLogger;
            private readonly IImmutableDictionary<string, object> _context;

            public DerivedLogger(StorageFolderLogger parentLogger, IImmutableDictionary<string, object> baseContext)
            {
                _parentLogger = parentLogger;
                _context = baseContext;
            }

            ILogger ILogger.AddContext(string label, object value)
            {
                if (string.IsNullOrWhiteSpace("label"))
                {
                    throw new ArgumentException("Must be characters", nameof(label));
                }
                if (_context.ContainsKey(label))
                {
                    throw new ArgumentException($"Context already contain '{label}'", nameof(label));
                }
                if (value != null)
                {
                    return new DerivedLogger(_parentLogger, _context.Add(label, value));
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
                return _parentLogger.WriteToStorageAsync();
            }
        }
        #endregion

        private static readonly int MAX_QUEUE_LENGTH = 100;
        private static readonly int MAX_BUFFER_SIZE = 1 * 1024 * 1024;
        private static readonly int MAX_BLOCKS = 50000;
        private static readonly TimeSpan MAX_TIME = TimeSpan.FromSeconds(3);

        private readonly Guid _sessionId = Guid.NewGuid();
        private readonly IStorageFacade _storageFacade;
        private readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions
        {
            IgnoreNullValues = true
        };
        private readonly ConcurrentQueue<object> _queue = new ConcurrentQueue<object>();
        private string? _blobName = null;
        private int _blocks = 0;
        private Task _writeTask = Task.CompletedTask;
        private DateTime? _lastWriteTime = null;
        private int _writeLockInt = 0;

        public StorageFolderLogger(IStorageFacade storageFacade)
        {
            _storageFacade = storageFacade;
        }

        ILogger ILogger.AddContext(string label, object value)
        {
            if (value != null)
            {
                var textValue = value.ToString();

                if (textValue != null)
                {
                    return new DerivedLogger(
                        this,
                        ImmutableDictionary<string, object>.Empty.Add(label, textValue));
                }
            }

            return this;
        }

        void ILogger.Display(string text)
        {
            Display(text, ImmutableDictionary<string, object>.Empty);
        }

        void ILogger.DisplayError(Exception exception)
        {
            DisplayError(exception, ImmutableDictionary<string, object>.Empty);
        }

        void ILogger.WriteEvent(string eventName)
        {
            WriteEvent(eventName, ImmutableDictionary<string, object>.Empty);
        }

        async Task ILogger.FlushAsync()
        {
            while (_queue.Count > 0)
            {
                await WriteToStorageAsync();
            }
        }

        private void Display(string text, IImmutableDictionary<string, object> context)
        {
            Console.WriteLine(text);

            PushLog("display", new { Text = text }, context);
        }

        private void DisplayError(Exception exception, IImmutableDictionary<string, object> context)
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

        private void WriteEvent(string eventName, IImmutableDictionary<string, object> context)
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
            IImmutableDictionary<string, object>? context)
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

            _queue.Enqueue(telemetry);
            if (_writeTask.Status == TaskStatus.RanToCompletion)
            {
                if (_queue.Count > MAX_QUEUE_LENGTH
                    || (_lastWriteTime != null && now.Subtract(_lastWriteTime.Value) > MAX_TIME))
                {
                    _writeTask = Task.Run(() => WriteToStorageAsync());
                }
            }
        }

        private async Task WriteToStorageAsync()
        {
            if (Interlocked.CompareExchange(ref _writeLockInt, 1, 0) != 0)
            {
                try
                {
                    bool isFirstLoop = true;

                    while ((isFirstLoop && _queue.Count > 0)
                        || _queue.Count > MAX_QUEUE_LENGTH)
                    {
                        var stream = new MemoryStream();
                        object? telemetry;

                        while (stream.Length < MAX_BUFFER_SIZE
                            && _queue.TryDequeue(out telemetry))
                        {
                            if (telemetry == null)
                            {
                                throw new NullReferenceException("Logged telemetry can't be null");
                            }
                            await JsonSerializer.SerializeAsync(stream, telemetry, _serializerOptions);
                            stream.WriteByte((byte)'\n');
                        }
                        stream.Position = 0;
                        if (_blobName == null)
                        {
                            _blobName = CreateBlobName();
                            await _storageFacade.CreateAppendBlobAsync(_blobName);
                        }
                        await _storageFacade.AppendBlobAsync(_blobName, stream);
                        ++_blocks;
                        if (_blocks == MAX_BLOCKS)
                        {
                            _blobName = null;
                        }
                        isFirstLoop = false;
                    }
                }
                finally
                {
                    _writeLockInt = 0;
                }
            }
        }

        private static string CreateBlobName()
        {
            var now = DateTime.Now;

            return $"{now.Year}/{now.Month,2:D2}/{now.Day,2:D2}/"
                + $"{now.Hour,2:D2}.{now.Minute,2:D2}.{now.Second,2:D2}.json";
        }
    }
}