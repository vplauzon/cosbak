using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Logging
{
    public interface ILogger
    {
        Task WriteAsync(TelemetryBase telemetry);

        Task FlushAsync();
    }
}