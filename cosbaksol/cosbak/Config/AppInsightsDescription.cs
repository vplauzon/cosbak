﻿namespace Cosbak.Config
{
    public class AppInsightsDescription
    {
        public string Key { get; set; }

        public string Role { get; set; }

        internal void Validate()
        {
            if (string.IsNullOrWhiteSpace(Key))
            {
                throw new BackupException("App Insights key is required");
            }
            if (string.IsNullOrWhiteSpace(Role))
            {
                throw new BackupException("App Insights role is required");
            }
        }
    }
}