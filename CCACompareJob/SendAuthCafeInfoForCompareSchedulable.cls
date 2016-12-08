/****************************************************************************************
Name              : SendAuthCafeInfoForCompareSchedulable
Description       : scheduable class for SendAuthCafeInfoForCompareBatchable
Created/Modified by      Created/Modified Date                 Related Task/Issue             
----------------------------------------------------------------------------------------
1. Mark Meyer            11/16/2016                            Version 1
****************************************************************************************/
global class SendAuthCafeInfoForCompareSchedulable implements Schedulable {
	global void execute(SchedulableContext sc) {
        Database.Batchable<sObject> job = new SendAuthCafeInfoForCompareBatchable();
        Database.executeBatch(job, 50);
	}
}