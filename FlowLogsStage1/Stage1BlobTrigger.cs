using System;
using System.IO;
using System.Collections.Generic;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public static class Stage1BlobTrigger
    {
        const int MAXDOWNLOADBYTES = 1024000;

        [FunctionName("Stage1BlobTrigger")]
        public static async Task Run(
            [BlobTrigger("%blobContainerName%/resourceId=/SUBSCRIPTIONS/{subId}/RESOURCEGROUPS/{resourceGroup}/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/{nsgName}/y={blobYear}/m={blobMonth}/d={blobDay}/h={blobHour}/m={blobMinute}/macAddress={mac}/PT1H.json", Connection = "nsgSourceDataConnection")] AppendBlobClient myBlob,
            [Queue("stage1", Connection = "AzureWebJobsStorage")] ICollector<Chunk> outputChunks,
            string subId, string resourceGroup, string nsgName, string blobYear, string blobMonth, string blobDay, string blobHour, string blobMinute, string mac,
            ILogger log)
        {
            try
            {
                string nsgSourceDataAccount = Util.GetEnvironmentVariable("nsgSourceDataAccount");
                if (nsgSourceDataAccount.Length == 0)
                {
                    log.LogError("Value for nsgSourceDataAccount is required.");
                    throw new System.ArgumentNullException("nsgSourceDataAccount", "Please provide setting.");
                }

                string blobContainerName = Util.GetEnvironmentVariable("blobContainerName");
                if (blobContainerName.Length == 0)
                {
                    log.LogError("Value for blobContainerName is required.");
                    throw new System.ArgumentNullException("blobContainerName", "Please provide setting.");
                }

                var blobDetails = new BlobDetails(subId, resourceGroup, nsgName, blobYear, blobMonth, blobDay, blobHour, blobMinute, mac);


                string storageConnectionString = Util.GetEnvironmentVariable("AzureWebJobsStorage");
                // Create a TableClient instance
                TableClient tableClient = new TableClient(storageConnectionString, "checkpoints");
                // Create table if not exist
                await tableClient.CreateIfNotExistsAsync();

                // get checkpoint
                Checkpoint checkpoint = await Checkpoint.GetCheckpoint(blobDetails, tableClient);
                // break up the block list into 10k chunks

                var blobProperties = await myBlob.GetPropertiesAsync();

                // get checkpoint
                // Checkpoint checkpoint = Checkpoint.GetCheckpoint(blobDetails, tableClient);

                // break up the block list into 10k chunks
                List<Chunk> chunks = new List<Chunk>();
                long currentChunkSize = 0;
                string currentChunkLastBlockName = "";
                long currentStartingByteOffset = 0;

                bool firstBlockItem = true;
                bool foundStartingOffset = false;
                bool tieOffChunk = false;

                int numberOfBlocks = 0;
                long sizeOfBlocks = 0;

                foreach (var blockListItem in myBlob.DownloadBlockList(BlockListingFilter.Committed))
                {
                    if (!foundStartingOffset)
                    {
                        if (firstBlockItem)
                        {
                            currentStartingByteOffset += blockListItem.Length;
                            firstBlockItem = false;
                            if (checkpoint.LastBlockName == "")
                            {
                                foundStartingOffset = true;
                            }
                        }
                        else
                        {
                            if (blockListItem.Name == checkpoint.LastBlockName)
                            {
                                foundStartingOffset = true;
                            }
                            currentStartingByteOffset += blockListItem.Length;
                        }
                    }
                    else
                    {
                        // tieOffChunk = add current chunk to the list, initialize next chunk counters
                        // conditions to account for:
                        // 1) current chunk is empty & not the last block (size > 10 I think)
                        //   a) add blockListItem to current chunk
                        //   b) loop
                        // 2) current chunk is empty & last block (size < 10 I think)
                        //   a) do not add blockListItem to current chunk
                        //   b) loop terminates
                        //   c) chunk last added to the list is the last chunk
                        // 3) current chunk is not empty & not the last block
                        //   a) if size of block + size of chunk >10k
                        //     i) add chunk to list  <-- tieOffChunk
                        //     ii) reset chunk counters
                        //   b) add blockListItem to chunk
                        //   c) loop
                        // 4) current chunk is not empty & last block
                        //   a) add chunk to list  <-- tieOffChunk
                        //   b) do not add blockListItem to chunk
                        //   c) loop terminates
                        tieOffChunk = (currentChunkSize != 0) && ((blockListItem.Length < 10) || (currentChunkSize + blockListItem.Length > MAXDOWNLOADBYTES));
                        if (tieOffChunk)
                        {
                            // chunk complete, add it to the list & reset counters
                            chunks.Add(new Chunk
                            {
                                BlobName = blobContainerName + "/" + myBlob.Name,
                                Length = currentChunkSize,
                                LastBlockName = currentChunkLastBlockName,
                                Start = currentStartingByteOffset,
                                BlobAccountConnectionName = nsgSourceDataAccount
                            });
                            currentStartingByteOffset += currentChunkSize; // the next chunk starts at this offset
                            currentChunkSize = 0;
                            tieOffChunk = false;
                        }
                        if (blockListItem.Length > 10)
                        {
                            numberOfBlocks++;
                            sizeOfBlocks += blockListItem.Length;

                            currentChunkSize += blockListItem.Length;
                            currentChunkLastBlockName = blockListItem.Name;
                        }
                    }

                }
                if (currentChunkSize != 0)
                {
                    // residual chunk
                    chunks.Add(new Chunk
                    {
                        BlobName = blobContainerName + "/" + myBlob.Name,
                        Length = currentChunkSize,
                        LastBlockName = currentChunkLastBlockName,
                        Start = currentStartingByteOffset,
                        BlobAccountConnectionName = nsgSourceDataAccount
                    });
                }

                if (chunks.Count > 0)
                {
                    var lastChunk = chunks[chunks.Count - 1];
                    // checkpoint.PutCheckpointActivity(tableClient, blobSize);
                    checkpoint.PutCheckpoint(tableClient, lastChunk.LastBlockName, lastChunk.Start + lastChunk.Length);
                }

                // add the chunks to output queue
                // they are sent automatically by Functions configuration
                foreach (var chunk in chunks)
                {
                    outputChunks.Add(chunk);
                    if (chunk.Length == 0)
                    {
                        log.LogError("chunk length is 0");
                    }
                }
            }
            catch (Exception e)
            {
                log.LogError(e, "Function Stage1BlobTrigger is failed to process request");
            }
        }
    }
}
