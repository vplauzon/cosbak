using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Logging
{
    public interface ILogger
    {
        void Write(TelemetryBase telemetry);

        Task FlushAsync();
    }
}