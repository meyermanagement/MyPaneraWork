/****************************************************************************************
Name              : SendCCAInfoForCompareScheduable
Description       : scheduable class for SendCCAInfoForCompareBatchable
Created/Modified by      Created/Modified Date                 Related Task/Issue             
----------------------------------------------------------------------------------------
1. Mark Meyer            11/16/2016                            Version 1
****************************************************************************************/
global class SendCCAInfoForCompareScheduable implements Schedulable {
	global void execute(SchedulableContext sc) {
        Database.Batchable<sObject> job = new SendCCAInfoForCompareBatchable();
        Database.executeBatch(job, 50);
	}
}