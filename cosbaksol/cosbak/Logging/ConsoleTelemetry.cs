using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Logging
{
    public class ConsoleTelemetry : TelemetryBase
    {
        public ConsoleTelemetry(string text) : base("console")
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                throw new ArgumentNullException(nameof(text));
            }

            Text = text;
        }

        public string Text { get; }
    }
}