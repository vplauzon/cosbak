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
        private readonly Guid _sessionId = Guid.NewGuid();
        private readonly IStorageFacade _storageFacade;
        //private string _blobName = null;
        private Stream _stream;
        private TextWriter _writer;

        public Logger(IStorageFacade storageFacade)
        {
            _storageFacade = storageFacade;
            _stream = new MemoryStream();
            _writer = new StreamWriter(_stream);
        }

        void ILogger.Display(
                string text,
                IImmutableDictionary<string, string> context)
        {
            Console.WriteLine(text);

            PushLog("display", new { Text = text }, context);
        }

        void ILogger.DisplayError(
            Exception exception,
            IImmutableDictionary<string, string> context)
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
            IImmutableDictionary<string, string> context,
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

        Task ILogger.FlushAsync()
        {
            throw new NotImplementedException();
        }

        private void PushLog(
            string eventType,
            object content,
            IImmutableDictionary<string, string> context)
        {
            var telemetry = new
            {
                SessionId = _sessionId,
                eventType,
                content,
                context
            };
            var serializer = new JsonSerializer();

            lock (_stream)
            {
                serializer.Serialize(_writer, telemetry);
            }
        }
    }
}