/****************************************************************************************
Name              : SendAuthUserInfoForCompareBatchable
Description       : Look for users that were updated or created in the last week for comparison
Created/Modified by      Created/Modified Date                 Related Task/Issue             
----------------------------------------------------------------------------------------
1. Mark Meyer            11/16/2016                            Version 1
****************************************************************************************/
global class SendAuthUserInfoForCompareBatchable implements Database.Batchable<sObject>, Database.AllowsCallouts  {
	
	String query;
	private static String DEFAULT_QUERY = 'SELECT Id, Status__c, First_Name__c, Last_Name__c, Account__c, User_Oracle_Id__c, CCA_Number__c, CCD_Account_Number__c, LastModifiedDate FROM Authorized_User__c WHERE LastModifiedDate = LAST_N_Days:7 AND (Status__c = \'Active\' OR Status__c = \'Inactive\') AND User_Oracle_Id__c != null';
	private FinanceServices service;

	global SendAuthUserInfoForCompareBatchable() {
		this(DEFAULT_QUERY);
	}
	
	global SendAuthUserInfoForCompareBatchable(String q) {
		System.assert(q != null, 'Query cannot be null');
        this.query = q;
	}

	global Database.QueryLocator start(Database.BatchableContext BC) {
		System.debug('>>>>>> query: ' + query);
		return Database.getQueryLocator(query);
	}

   	global void execute(Database.BatchableContext BC, List<sObject> scope) {
   		System.debug('>>>>>> Processing batch with ' + scope.size() + ' authorized users');
		Authorized_User__c[] users = (Authorized_User__c[])scope;
		FinanceServices service = new FinanceServices();
		service.sendAuthorizedUsersForCompare(users);
	}
	
	global void finish(Database.BatchableContext BC) {
		
	}
	
}