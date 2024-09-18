using Azure;
using System;
using Azure.Data.Tables;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public class Checkpoint : ITableEntity
    {

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        public string LastBlockName { get; set; }
        public long StartingByteOffset { get; set; }

        public Checkpoint()
        {
        }

        public Checkpoint(string partitionKey, string rowKey, string blockName, long offset)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            LastBlockName = blockName;
            StartingByteOffset = offset;
        }

        public static async Task<Checkpoint> GetCheckpoint(BlobDetails blobDetails, TableClient tableClient)
        {
            try
            {
                return await tableClient.GetEntityAsync<Checkpoint>(blobDetails.GetPartitionKey(), blobDetails.GetRowKey());
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // If the entity is not found, return a new Checkpoint
                return new Checkpoint(blobDetails.GetPartitionKey(), blobDetails.GetRowKey(), "", 0);
            }
        }

        public static async Task<Checkpoint> GetCheckpointActivity(BlobDetailsActivity blobDetails, TableClient checkpointTable)
        {
            try
            {
                return await checkpointTable.GetEntityAsync<Checkpoint>(blobDetails.GetPartitionKey(), blobDetails.GetRowKey());
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // If the entity is not found, return a new Checkpoint
                return new Checkpoint(blobDetails.GetPartitionKey(), blobDetails.GetRowKey(), "", 0);
            }
        }

        public async Task PutCheckpoint(TableClient tableClient, string lastBlockName, long startingByteOffset)
        {
            LastBlockName = lastBlockName;
            StartingByteOffset = startingByteOffset;

            await tableClient.UpsertEntityAsync(this);
        }

        public async void PutCheckpointActivity(TableClient tableClient, long startingByteOffset)
        {
            StartingByteOffset = startingByteOffset;

            await tableClient.UpsertEntityAsync(this);
        }
    }
}
