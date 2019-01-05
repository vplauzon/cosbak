namespace Cosbak.Config
{
    public class AppInsightsDescription
    {
        public string Key { get; set; }

        public string Role { get; set; }

        internal void Validate()
        {
            if (string.IsNullOrWhiteSpace(Key))
            {
                throw new BackupException("App Insights description must have a key");
            }
            if (string.IsNullOrWhiteSpace(Role))
            {
                throw new BackupException("App Insights description must have a role");
            }
        }
    }
}