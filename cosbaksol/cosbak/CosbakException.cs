using System;

namespace Cosbak
{
    public class CosbakException : Exception
    {
        public CosbakException(string message) : base(message)
        {
        }
    }
}