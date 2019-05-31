using Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cosbak
{
    public struct DocumentHash
    {
        public DocumentHash(string id, object partitionKey)
        {
            if (string.IsNullOrWhiteSpace(id))
            {
                throw new ArgumentNullException(nameof(id));
            }
            IdHash = id.GetHashCode();
            PartitionHash = partitionKey == null
                ? 0
                : partitionKey.GetHashCode();
        }

        private DocumentHash(int idHash, int partitionHash)
        {
            IdHash = idHash;
            PartitionHash = partitionHash;
        }

        public int IdHash { get; }

        public int PartitionHash { get; }

        public static DocumentHash Read(BinaryReader reader)
        {
            var idHash = reader.ReadInt32();
            var partitionHash = reader.ReadInt32();

            return new DocumentHash(idHash, partitionHash);
        }

        public void Write(BinaryWriter writer)
        {
            writer.Write(IdHash);
            writer.Write(PartitionHash);
        }

        #region Object methods
        public override string ToString()
        {
            return $"{{{IdHash}, {PartitionHash}}}";
        }

        public override bool Equals(object obj)
        {
            DocumentHash? hash = obj as DocumentHash?;

            return hash != null
                && hash.Value.IdHash == IdHash
                && hash.Value.PartitionHash == PartitionHash;
        }

        public override int GetHashCode()
        {
            long longId = IdHash;
            long longPartition = PartitionHash;
            long pushedPartition = longPartition << 32;
            long compoundHash = pushedPartition | longId;

            return compoundHash.GetHashCode();
        }
        #endregion
    }
}