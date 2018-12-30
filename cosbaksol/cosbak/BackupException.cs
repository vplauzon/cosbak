using System;

namespace cosbak
{
    public class BackupException : Exception
    {
        public BackupException(string message) : base(message)
        {
        }
    }
}