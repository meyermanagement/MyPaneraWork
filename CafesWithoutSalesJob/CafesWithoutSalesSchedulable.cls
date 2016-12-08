/**********************************************
Name: CafesWithoutSalesSchedulable
Description: Scheduler for CafesWithoutSales batch job

Created/Modified by        Created/Modified Date        Notes             
----------------------------------------------------------------------------------------
1. Mark Meyer              07/21/2016                   Version 1.0

**********************************************/
global class CafesWithoutSalesSchedulable implements Schedulable {
    global void execute(SchedulableContext SC) {
    	Date yesterday = System.today().addDays(-1);
    	String[] emailrecipients = CommonUtils.getSupportEmails();
    	CafesWithoutSales.sendCafesWithoutSalesEmail(yesterday, emailrecipients); 
   }
}