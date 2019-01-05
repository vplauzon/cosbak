using System;

namespace Cosbak
{
    public class BackupException : Exception
    {
        public BackupException(string message) : base(message)
        {
        }
    }
}