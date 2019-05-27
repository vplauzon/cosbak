using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cosbak.Logging
{
    public class Logger : ILogger
    {
        private readonly IStorageFacade _storageFacade;

        public Logger(IStorageFacade storageFacade)
        {
            _storageFacade = storageFacade;
        }

        void ILogger.Write(TelemetryBase telemetry)
        {
            if (telemetry == null)
            {
                throw new ArgumentNullException(nameof(telemetry));
            }

            var consoleTelemetry = telemetry as ConsoleTelemetry;

            if (consoleTelemetry != null)
            {
                Console.WriteLine(consoleTelemetry.Text);
            }

            throw new NotImplementedException();
        }

        Task ILogger.FlushAsync()
        {
            throw new NotImplementedException();
        }
    }
}