using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Cosbak.Storage
{
    public class BlobLease : IDisposable
    {
        private static readonly TimeSpan DEFAULT_LEASE_DURATION = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan DEFAULT_LEASE_RENEWAL_PERIOD = TimeSpan.FromSeconds(20);

        private readonly CloudBlob _blob;
        private readonly string _leaseId;
        private readonly Timer _timer;

        public BlobLease(CloudBlob blob, string leaseId)
        {
            _blob = blob;
            _leaseId = leaseId;
            _timer = new Timer(DEFAULT_LEASE_RENEWAL_PERIOD.TotalMilliseconds);
            _timer.Elapsed += OnElapsed;
            _timer.Enabled = true;
        }

        public string LeaseId
        {
            get => _leaseId;
        }

        public static async Task<BlobLease?> CreateLeaseAsync(CloudBlob blob)
        {
            try
            {
                var leaseId = await blob.AcquireLeaseAsync(DEFAULT_LEASE_DURATION);

                return new BlobLease(blob, leaseId);
            }
            catch (StorageException)
            {
                return null;
            }
        }

        public async Task ReleaseLeaseAsync()
        {
            _timer.Enabled = false;
            await _blob.ReleaseLeaseAsync(new AccessCondition
            {
                LeaseId = _leaseId
            });
        }

        void IDisposable.Dispose()
        {
            _timer.Enabled = false;
            _timer.Dispose();
        }

        private void OnElapsed(object sender, ElapsedEventArgs e)
        {
            var task = _blob.RenewLeaseAsync(new AccessCondition
            {
                LeaseId = _leaseId
            });

            task.Wait();
        }
    }
}