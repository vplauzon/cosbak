using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace Cosbak.Controllers
{
    public class CollectionFilter
    {
        private readonly IImmutableDictionary<string, IImmutableSet<string>> _filterMap;

        public CollectionFilter(IEnumerable<string> filters)
        {
            _filterMap = CreateFilterMap(filters);
        }

        public bool IsIncluded(string db, string collection)
        {
            if (!_filterMap.Any())
            {
                return true;
            }
            else if (!_filterMap.ContainsKey(db))
            {
                return false;
            }
            else
            {
                var collectionFilter = _filterMap[db];

                return collectionFilter.Contains("*")
                    || collectionFilter.Contains(collection);
            }
        }

        private static IImmutableDictionary<string, IImmutableSet<string>> CreateFilterMap(
            IEnumerable<string> filters)
        {
            if (filters == null)
            {
                return ImmutableDictionary<string, IImmutableSet<string>>.Empty;
            }
            else
            {
                var list = from f in filters
                           let parts = f.Split(".")
                           let db = parts[0]
                           let collection = parts[1].Trim()
                           group collection by db;
                var dbMap = list.ToImmutableDictionary(
                    g => g.Key,
                    g => g.ToImmutableHashSet() as IImmutableSet<string>);

                return dbMap;
            }
        }
    }
}