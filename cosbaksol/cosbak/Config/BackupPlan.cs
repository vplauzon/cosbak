using System.Linq;

namespace Cosbak.Config
{
    public class BackupPlan
    {
        public string[] Filters { get; set; }

        public BackupFrequencies Frequencies { get; set; }

        public void Validate()
        {
            if (Filters != null)
            {
                if (Filters.Any(s => string.IsNullOrWhiteSpace(s)))
                {
                    throw new BackupException("Cosmos filters are optional ; if present they must be non-empty");
                }

                var untrimmed = from f in Filters
                                where f != f.Trim()
                                select f;

                if (untrimmed.Any())
                {
                    throw new BackupException($"Cosmos filter has leading or trailing spaces: '{untrimmed.First()}'");
                }

                var repeat = from g in (from f in Filters
                                        group f by f into g
                                        select g)
                             where g.Count() > 1
                             select g.Key;

                if (repeat.Any())
                {
                    throw new BackupException($"Cosmos filter is repeated:  '{repeat.First()}'");
                }

                var splitted = from f in Filters
                               select f.Split('.');

                if (splitted.Any(s => s.Length > 2))
                {
                    throw new BackupException("Cosmos filters can't have more than one dot as it represents <DB NAME> or <DB NAME>.<COLLECTION>");
                }

                var grouping = from p in splitted
                               group p by p[0] into g
                               select g;
                var collectionAndNot = from g in grouping
                                       let hasNoCollection = g.Any(p => p.Length == 1)
                                       let hasCollection = g.Any(p => p.Length == 2)
                                       where hasNoCollection && hasCollection
                                       select g.Key;

                if (collectionAndNot.Any())
                {
                    throw new BackupException($"Cosmos filter contain both a database and some of its collections:  '{collectionAndNot.First()}'");
                }
            }
        }
    }
}