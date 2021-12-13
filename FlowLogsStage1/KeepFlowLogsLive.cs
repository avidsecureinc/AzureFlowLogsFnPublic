using System.IO;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Threading.Tasks;
namespace NwNsgProject
{
    public static class KeepFlowLogsLive
    {
		[FunctionName("KeepFlowLogsLive")]
		public static async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer, TraceWriter log)
		{
		    if(myTimer.IsPastDue)
		    {
		        log.Info("Timer is running late!");
		    }
		}
	}

}