@isTest
public class CafesWithoutSales_Test {

    static testMethod void testNumberOfCafesWithoutSales() {
        TestServices.insertEmptyUser();
        TestServices.insertCronUser();

        List<Opportunity> sales =  new List<Opportunity>();
        
        //Setup data
        B_C__c cafe1 = new B_C__c(Name='Test Cafe1', Caf_Number__c='123', Open_Date__c=System.today().addDays(-30));
        B_C__c cafe2 = new B_C__c(Name='Test Cafe2', Caf_Number__c='456', Open_Date__c=System.today());
        insert new B_c__c[]{cafe1, cafe2};

        for (Integer i = 1; i <= 5;  i++) {
            sales.add(new Opportunity(Name='Sale1', StageName='Closed Won', CloseDate=System.today(), Amount=50, Customer_Hub_B_C_Id__c='123', Total_Sale_Amount__c = 50));
            sales.add(new Opportunity(Name='Sale2', StageName='Closed Won', CloseDate=System.today(), Amount=100, Customer_Hub_B_C_Id__c='123', Total_Sale_Amount__c = 100));
        }
        insert sales;

        //Select all cafes
        B_C__c[] cafes = [SELECT Id, Name, Average_Num_Orders_YTD__c, Open_Date__c FROM B_C__c];
        Map<Id, B_C__c> cafeIdToCafeMap = new Map<Id, B_C__c>();
		if(!cafes.isEmpty()) {
	        //Gather cafe ids into map
	        for (B_C__c cafe : cafes) {
	            cafeIdToCafeMap.put(cafe.id, cafe);
	        }
		}

		//Remove cafes with sales
		for (AggregateResult ar : [SELECT B_C__c cafeId FROM Opportunity GROUP BY B_C__c]){
            String cafeId = String.valueOf(ar.get('cafeId'));
            cafeIdToCafeMap.remove(cafeId);
   		}
        
   		//Send Email
   		CafesWithoutSales.sendCafesWithoutSalesEmail(System.today(), new String[] {'noemail@panerabread.com'});

        //Verify expected results
        for (Id cafeNoSalesId : cafeIdToCafeMap.keySet()){
        	B_C__c cafeNoSales = cafeIdToCafeMap.get(cafeNoSalesId);
        	System.assertEquals('Test Cafe2', cafeNoSales.Name, 'Cafes without sales name is incorrect.');
        	System.assertEquals(0, cafeNoSales.Average_Num_Orders_YTD__c, 'Cafes without sales average number of orders are incorrect.');
    		System.assertEquals(System.today(), cafeNoSales.Open_Date__c, 'Cafes without sales open date is incorrect.');
    	}
    }
}