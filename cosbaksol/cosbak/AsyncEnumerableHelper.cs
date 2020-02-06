using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak
{
    internal static class AsyncEnumerableHelper
    {
        public async static Task<IEnumerable<T>> ToEnumerable<T>(this IAsyncEnumerable<T> source)
        {
            var list = new List<T>();

            await foreach (var i in source)
            {
                list.Add(i);
            }

            return list;
        }
    }
}