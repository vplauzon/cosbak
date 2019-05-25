using System;
using System.Collections.Generic;

namespace Cosbak.Logging
{
    public class MetricTelemetry : TelemetryBase
    {
        public MetricTelemetry(string name, double value) : base("metric")
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Value = value;
        }

        public MetricTelemetry(string name, int count) : base("metric")
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Count = count;
        }

        public MetricTelemetry(string name, TimeSpan duration) : base("metric")
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Duration = duration;
        }

        public string Name { get; }

        public double? Value { get; }

        public int? Count { get; }

        public TimeSpan? Duration { get; }
    }
}