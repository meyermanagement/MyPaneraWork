/**********************************************
Name: CafesWithoutSales
Description: Email list of cafes without sales yesterday

Created/Modified by        Created/Modified Date        Notes             
----------------------------------------------------------------------------------------
1. Mark Meyer              07/21/2016                   Version 1.0
**********************************************/
public class CafesWithoutSales{

	@future
	public static void  sendCafesWithoutSalesEmail(Date d, String[] recipients){

		//Select all cafes
		B_C__c[] cafes = [SELECT Id, Name, Average_Num_Orders_YTD__c, Open_Date__c FROM B_C__c WHERE Close_Date__c = null AND Open_Date__c <= TODAY ORDER BY Average_Num_Orders_YTD__c DESC];
		Map<Id, B_C__c> cafeIdToCafeMap = new Map<Id, B_C__c>();

		system.debug('>>>>>> cafes size:' + cafes.size());
		if(!cafes.isEmpty()) {
	        //Gather cafe ids into map
	        for (B_C__c cafe : cafes) {
	            cafeIdToCafeMap.put(cafe.id, cafe);
	        }
		}

        //Select cafes with sales and remove from cafe map
        for (AggregateResult ar :    [SELECT B_C__c cafeId FROM Opportunity 
                                      WHERE CloseDate = :d
                                      AND B_C__c in :cafeIdToCafeMap.keySet()
                                      group by B_C__c]){
            
            String cafeId = String.valueOf(ar.get('cafeId'));
            cafeIdToCafeMap.remove(cafeId);
   		}
	  	
	  	//Send email
    	String tdStyle = ' style="border:1px solid #888888;padding:5px;" ';
		String emailSubject = 'Cafes Without Sales for '+System.today().format();
		String emailBody = '<table width="400px" style="border:1px solid #888888;border-collapse:collapse;">';
        emailBody += '<tr>';
        emailBody +=   '<th' + tdStyle + '><strong>Cafe Number</strong></th>';
        emailBody +=   '<th' + tdStyle + '><strong>Average # of Daily Sales</strong></th>';
        emailBody +=   '<th' + tdStyle + '><strong>Open Date</strong></th>';
        emailBody += '</tr>';
        //Loop through cafes left in list and display average daily sales count and open date
        for (Id cafeNoSalesId : cafeIdToCafeMap.keySet()){
        	B_C__c cafeNoSales = cafeIdToCafeMap.get(cafeNoSalesId);
        	emailBody += '<tr>';
            emailBody +=   '<td' + tdStyle + '>' + cafeNoSales.Name + '</td>';
            emailBody +=   '<td' + tdStyle + '>' + cafeNoSales.Average_Num_Orders_YTD__c + '</td>';
            emailBody +=   '<td' + tdStyle + '>' + cafeNoSales.Open_Date__c + '</td>';
            emailBody += '</tr>';
        }
        emailBody += '</table>';
        CommonUtils.sendHtmlEmail(emailSubject, emailBody, recipients, CommonUtils.getSFTechnicalHelpEmailId());
    }
}