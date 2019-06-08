using System;
using System.Collections.Generic;
using System.Linq;

namespace Cosbak.Config
{
    public static class FilterHelper
    {
        public static void ValidateFilters(string[] filters)
        {
            if (filters != null)
            {
                if (filters.Any(s => string.IsNullOrWhiteSpace(s)))
                {
                    throw new CosbakException("Collection filters are optional ; if present they must be non-empty");
                }

                var trimmed = from f in filters
                              select f.Trim();
                var repeat = from g in (from f in trimmed
                                        group f by f into g
                                        select g)
                             where g.Count() > 1
                             select g.Key;

                if (repeat.Any())
                {
                    throw new CosbakException($"Collection filter is repeated:  '{repeat.First()}'");
                }

                var nonTwoParters = from f in trimmed
                                    where f.Split('.').Length != 2
                                    select f;

                if (nonTwoParters.Any())
                {
                    throw new CosbakException(
                        $" '{nonTwoParters.First()}':  "
                        + "Collection filters must have one and only one dot as it represents "
                        + "<DB NAME>.<COLLECTION> (or <DB NAME>.*)");
                }
            }
        }
    }
}