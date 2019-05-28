using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak.Logging
{
    public interface ILogger
    {
        void Display(string text);

        void DisplayError(Exception exception);

        void WriteEvent(
            string eventName,
            IImmutableDictionary<string, string> properties = null,
            double? metric = null,
            long? count = null,
            TimeSpan? duration = null);

        Task FlushAsync();
    }
}