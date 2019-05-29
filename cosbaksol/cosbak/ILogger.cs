using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak
{
    public interface ILogger
    {
        void Display(
            string text,
            IImmutableDictionary<string, string> context = null);

        void DisplayError(
            Exception exception,
            IImmutableDictionary<string, string> context = null);

        void WriteEvent(
            string eventName,
            IImmutableDictionary<string, string> context = null,
            double? metric = null,
            long? count = null,
            TimeSpan? duration = null);

        Task FlushAsync();
    }
}