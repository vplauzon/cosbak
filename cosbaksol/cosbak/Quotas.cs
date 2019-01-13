using System;
using System.Collections.Generic;
using System.Text;

namespace Cosbak
{
    internal static class Quotas
    {
        public const long MAX_INDEX_SIZE = (long)20 * 1024 * 1024;

        public const long MAX_CONTENT_BUFFER_SIZE = (long)100 * 1024 * 1024;
    }
}