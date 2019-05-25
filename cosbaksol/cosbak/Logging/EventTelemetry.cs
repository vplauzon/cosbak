using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Cosbak.Logging
{
    public class EventTelemetry : TelemetryBase
    {
        public EventTelemetry(
            string name,
            IImmutableDictionary<string, string> properties = null) : base("metric")
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Name = name;
            Properties = properties;
        }

        public EventTelemetry(
            string name,
            double value,
            IImmutableDictionary<string, string> properties = null) : this(name, properties)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Value = value;
        }

        public EventTelemetry(
            string name,
            long count,
            IImmutableDictionary<string, string> properties = null) : this(name, properties)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Count = count;
        }

        public EventTelemetry(
            string name,
            TimeSpan duration,
            IImmutableDictionary<string, string> properties = null) : this(name, properties)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Duration = duration;
        }

        public string Name { get; }

        public IImmutableDictionary<string, string> Properties { get; }

        public double? Value { get; }

        public long? Count { get; }

        public TimeSpan? Duration { get; }
    }
}