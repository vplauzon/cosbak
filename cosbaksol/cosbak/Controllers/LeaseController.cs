using Cosbak.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace Cosbak.Controllers
{
    internal class LeaseController
    {
        private readonly ILogger _logger;
        private readonly IStorageFacade _storageFacade;

        public LeaseController(ILogger logger, IStorageFacade storageFacade)
        {
            _logger = logger;
            _storageFacade = storageFacade;
        }

        public async Task<BlobLease?> AcquireLeaseAsync(string blobName, TimeSpan timeout)
        {
            var watch = new Stopwatch();
            var waitTime = TimeSpan.FromSeconds(1);

            watch.Start();

            while (watch.Elapsed < timeout)
            {
                var lease = await _storageFacade.AcquireLeaseAsync(blobName);

                if (lease != null)
                {
                    return lease;
                }
                else
                {
                    waitTime = Min(timeout - watch.Elapsed, waitTime);
                    _logger.Display($"Blob {blobName} is locked for {watch.Elapsed}"
                        + $", retry in {waitTime}");
                    _logger
                        .AddContext("duration", watch.Elapsed)
                        .WriteEvent("blob-locked");
                    await Task.Delay(waitTime);
                    waitTime = Min(waitTime * 2, TimeSpan.FromSeconds(5));
                }
            }

            _logger.Display($"Blob {blobName} is locked, timeout expired");

            return null;
        }

        private static TimeSpan Min(TimeSpan t1, TimeSpan t2)
        {
            return t1 < t2
                ? t1
                : t2;
        }
    }
}