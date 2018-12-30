using System;

namespace cosbak.Config
{
    public class AccountDescription
    {
        public Uri Uri { get; set; }

        public string Key { get; set; }

        public string[] Filters { get; set; }
    }
}