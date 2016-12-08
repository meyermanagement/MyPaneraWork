/****************************************************************************************
Name              : SendAuthCafeInfoForCompareBatchable
Description       : Look for cafes that were updated or created in the last week for comparison
Created/Modified by      Created/Modified Date                 Related Task/Issue             
----------------------------------------------------------------------------------------
1. Mark Meyer            11/16/2016                            Version 1
****************************************************************************************/
global class SendAuthCafeInfoForCompareBatchable implements Database.Batchable<sObject>, Database.AllowsCallouts  {
	
	String query;
	private static String DEFAULT_QUERY = 'SELECT Id, Cafe_Oracle_Id__c, Cafe_Number__c, CCA_Number__c, Status__c, cca_organization_name__c, LastModifiedDate FROM Authorized_Cafe__c WHERE LastModifiedDate = LAST_N_Days:7 AND (Status__c = \'Active\' OR Status__c = \'Inactive\')'; 
	private FinanceServices service;

	global SendAuthCafeInfoForCompareBatchable() {
		this(DEFAULT_QUERY);
	}
	
	global SendAuthCafeInfoForCompareBatchable(String q) {
		System.assert(q != null, 'Query cannot be null');
        this.query = q;
	}

	global Database.QueryLocator start(Database.BatchableContext BC) {
		System.debug('>>>>>> query: ' + query);
		return Database.getQueryLocator(query);
	}

   	global void execute(Database.BatchableContext BC, List<sObject> scope) {
   		System.debug('>>>>>> Processing batch with ' + scope.size() + ' authorized cafes');
   		Authorized_Cafe__c[] cafes = (Authorized_Cafe__c[])scope;
   		FinanceServices service = new FinanceServices();
   		service.sendAuthorizedCafesForCompare(cafes);
	}
	
	global void finish(Database.BatchableContext BC) {
		
	}
	
}