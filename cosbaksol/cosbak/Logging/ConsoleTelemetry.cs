using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Logging
{
    public class ConsoleTelemetry : TelemetryBase
    {
        #region Constructors
        public static ConsoleTelemetry Log(string text)
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                throw new ArgumentNullException(nameof(text));
            }

            return new ConsoleTelemetry(text);
        }

        private ConsoleTelemetry(string text) : base("console")
        {
            Text = text;
        }
        #endregion

        public string Text { get; }
    }
}