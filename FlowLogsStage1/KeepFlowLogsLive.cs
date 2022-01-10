using System.IO;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
namespace NwNsgProject
{
    public static class KeepFlowLogsLive
    {
		[FunctionName("KeepFlowLogsLive")]
		public static async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer, ILogger log)
		{
		    if(myTimer.IsPastDue)
		    {
		        log.LogInformation("Timer is running late!");
		    }
		}
	}

}