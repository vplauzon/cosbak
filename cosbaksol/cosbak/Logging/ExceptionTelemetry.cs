using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak.Logging
{
    public class ExceptionTelemetry : TelemetryBase
    {
        public ExceptionTelemetry(Exception exception) : base("error")
        {
            if (exception == null)
            {
                throw new ArgumentNullException(nameof(exception));
            }

            Exception = exception;
        }

        public Exception Exception { get; }
    }
}