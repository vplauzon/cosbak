using System;
using System.Collections.Immutable;
using System.Linq;

namespace Cosbak.Config
{
    public class BackupPlan
    {
        public IImmutableList<string> Filters { get; set; }

        public TimeSpan? StartFrom { get; set; }

        public BackupFrequencies Frequencies { get; set; }

        public void Validate()
        {
            if (Filters == null)
            {
                if (Filters.Any(s => string.IsNullOrWhiteSpace(s)))
                {
                    throw new CosbakException("Cosmos filters are optional ; if present they must be non-empty");
                }

                var trimmed = from f in Filters
                              select f.Trim();
                var repeat = from g in (from f in trimmed
                                        group f by f into g
                                        select g)
                             where g.Count() > 1
                             select g.Key;

                if (repeat.Any())
                {
                    throw new CosbakException($"Cosmos filter is repeated:  '{repeat.First()}'");
                }

                var nonTwoParters = from f in trimmed
                                    where f.Split('.').Length != 2
                                    select f;

                if (nonTwoParters.Any())
                {
                    throw new CosbakException(
                        $" '{nonTwoParters.First()}':  "
                        + "Cosmos filters must have one and only one dot as it represents "
                        + "<DB NAME>.<COLLECTION> (or <DB NAME>.*)");
                }
            }
        }
    }
}