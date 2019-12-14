using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Cosbak
{
    public interface ILogger
    {
        ILogger AddContext<T>(string label, T value);

        void Display(string text);

        void DisplayError(Exception exception);

        void WriteEvent(string eventName);

        Task FlushAsync();
    }
}