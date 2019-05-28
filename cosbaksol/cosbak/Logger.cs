using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak
{
    public class Logger : ILogger
    {
        private readonly IStorageFacade _storageFacade;

        public Logger(IStorageFacade storageFacade)
        {
            _storageFacade = storageFacade;
        }

        void ILogger.Display(string text)
        {
            Console.WriteLine(text);

            throw new NotImplementedException();
        }

        void ILogger.DisplayError(Exception exception)
        {
            Console.Error.WriteLine($"Exception:  '{exception.GetType().Name}'");
            Console.Error.WriteLine($"Full Name:  '{exception.GetType().FullName}'");
            Console.Error.WriteLine($"Stack Trace:  '{exception.StackTrace}'");

            throw new NotImplementedException();
        }

        void ILogger.WriteEvent(
            string eventName,
            IImmutableDictionary<string, string> properties,
            double? metric,
            long? count,
            TimeSpan? duration)
        {
            throw new NotImplementedException();
        }

        Task ILogger.FlushAsync()
        {
            throw new NotImplementedException();
        }
    }
}