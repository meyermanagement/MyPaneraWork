/****************************************************************************************
Name              : SendAuthUserInfoForCompareSchedulable
Description       : scheduable class for SendAuthUserInfoForCompareBatchable
Created/Modified by      Created/Modified Date                 Related Task/Issue             
----------------------------------------------------------------------------------------
1. Mark Meyer            11/16/2016                            Version 1
****************************************************************************************/
global class SendAuthUserInfoForCompareSchedulable implements Schedulable {
	global void execute(SchedulableContext sc) {
        Database.Batchable<sObject> job = new SendAuthUserInfoForCompareBatchable();
        Database.executeBatch(job, 50);
	}
}