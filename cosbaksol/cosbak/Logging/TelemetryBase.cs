using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Logging
{
    public class TelemetryBase
    {
        public TelemetryBase(string eventType)
        {
            EventType = eventType;
        }

        public string EventType { get; }
    }
}