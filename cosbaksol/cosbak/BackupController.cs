using System;
using System.Threading.Tasks;
using Cosbak.Config;

namespace Cosbak
{
    public class BackupController
    {
        private readonly BackupDescription _description;

        public BackupController(BackupDescription description)
        {
            _description = description;
        }

        public async Task BackupAsync()
        {
            throw new NotImplementedException();
        }
    }
}