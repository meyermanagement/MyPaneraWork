/****************************************************************************************
Name              : SendCCAInfoForCompareBatchable
Description       : Look for CCA that were updated or created in the last week for Compare Job
Created/Modified by      Created/Modified Date                 Related Task/Issue             
----------------------------------------------------------------------------------------
1. Mark Meyer         	 11/16/2016                            Version 1
****************************************************************************************/
global class SendCCAInfoForCompareBatchable implements Database.Batchable<sObject>, Database.AllowsCallouts  {
	
	String query;
	private static String DEFAULT_QUERY = 'SELECT Id, CCA_Oracle_Id__c, CCA_Number__c, Taxpayer_Id__c, Org_Billing_City__c, Set_of_Books__c,'+
                                    'Org_Billing_Country__c, Org_is_Subaccount__c, Organization_Name__c, Primary_Organization_CCD_Number__c, Primary_Organization_CCA_Number__c,'+ 
                                    'PO_Number_Required__c, Primary_Organization_Name__c, Primary_Organization_Number__c, Primary_Taxpayer_ID__c, Send_Invoices_via_Email__c, Standard_PO_Number__c,' +
                                    'Org_Billing_State__c, Status__c, Org_Billing_Street_Address__c, Org_Billing_Street_Address_2__c, Subaccount_Department__c, Tax_Exempt__c,' +
                                    'Additional_Contact_Email_1__c, Additional_Contact_Email_2__c, Additional_Contact_Email_3__c, Additional_Contact_Email_4__c,' +
                                    'Additional_Contact_Name_1__c, Additional_Contact_Name_2__c, Additional_Contact_Name_3__c, Additional_Contact_Name_4__c,' +
                                    'Additional_Contact_Phone_1__c, Additional_Contact_Phone_2__c, Additional_Contact_Phone_3__c, Additional_Contact_Phone_4__c,' +
                                    'Email_Invoices_to_Contact__c, Email_Invoices_to_Additional_Contact_1__c, Email_Invoices_to_Additional_Contact_2__c, Email_Invoices_to_Additional_Contact_3__c, Email_Invoices_to_Additional_Contact_4__c,' +                                    
                                    'Contact_Email__c, Contact_Name__c, Contact_Phone__c, Requested_By_Name__c, Requested_Date__c,' +
                                    'Tax_Exempt_Number__c, Org_Billing_Zip__c, Org_Number__c, Start_Date__c, End_Date__c, Primary_Org_Unchanged__c, LastModifiedDate FROM CCA_Organization__c WHERE LastModifiedDate = LAST_N_Days:7 AND (Status__c != \'Rejected\' AND Status__c != \'Requested\') AND CCA_Oracle_Id__c != null';


	global SendCCAInfoForCompareBatchable() {
		this(DEFAULT_QUERY);
	}
	
	global SendCCAInfoForCompareBatchable(String q) {
		System.assert(q != null, 'Query cannot be null');
        this.query = q;
	}

	global Database.QueryLocator start(Database.BatchableContext BC) {
		System.debug('>>>>>> query: ' + query);
        return Database.getQueryLocator(query);
	}

   	global void execute(Database.BatchableContext BC, List<sObject> scope) {
   		System.debug('>>>>>> Processing batch with ' + scope.size() + ' ccas');
		CCA_Organization__c[] ccas = (CCA_Organization__c[])scope;
		FinanceServices service = new FinanceServices();
		service.sendCCACustomersForCompare(ccas);
	}
	
	global void finish(Database.BatchableContext BC) {
		
	}
	
}