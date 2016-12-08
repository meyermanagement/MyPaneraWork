@isTest
private class SendCCAInfoForCompare_Test {
	
	static CCA_Organization__c cca1;
	static CCA_Organization__c cca2;
	static Organization__c org;
	
	static void setup() {
		org = TestServices.getOrganization('Test');
		insert org;

		cca1 = TestServices.getCCAOrganization(org.id, 1, 'Approved');
		cca2 = TestServices.getCCAOrganization(org.id, 2, 'Approved');
		cca1.CCA_Oracle_Id__c = '00001';
		cca2.CCA_Oracle_Id__c = '00002';
		insert new List<CCA_Organization__c>{cca1,cca2};

		Remote_Site__c rs = new Remote_Site__c(Send_CCA_Customer_for_Compare_Url__c='http://www.foo1.bar/');
		insert rs;

		insert new List<CCA_Finance_Api_Fields__c>{
			new CCA_Finance_Api_Fields__c(Name='Id',Salesforce_Field__c='Id'),
			new CCA_Finance_Api_Fields__c(Name='cca_oracle_id',Salesforce_Field__c='Cca_Oracle_Id__c'),
			new CCA_Finance_Api_Fields__c(Name='cca_number',Salesforce_Field__c='cca_number__c'),
			new CCA_Finance_Api_Fields__c(Name='taxpayer_id',Salesforce_Field__c='taxpayer_id__c'),
			new CCA_Finance_Api_Fields__c(Name='status',Salesforce_Field__c='status__c'),
			new CCA_Finance_Api_Fields__c(Name='org_subacccount',Salesforce_Field__c='Org_is_Subaccount__c'),
			new CCA_Finance_Api_Fields__c(Name='primary_taxpayer_Id',Salesforce_Field__c='primary_taxpayer_Id__c'),
			new CCA_Finance_Api_Fields__c(Name='subaccount_dept',Salesforce_Field__c='Subaccount_Department__c'),
			new CCA_Finance_Api_Fields__c(Name='primary_organization',Salesforce_Field__c='primary_organization_name__c'),
			new CCA_Finance_Api_Fields__c(Name='primary_organization_ccd_number',Salesforce_Field__c='primary_organization_ccd_number__c'),
			new CCA_Finance_Api_Fields__c(Name='org_billing_street_address',Salesforce_Field__c='org_billing_street_address__c'),
			new CCA_Finance_Api_Fields__c(Name='org_billing_city',Salesforce_Field__c='org_billing_city__c'),
			new CCA_Finance_Api_Fields__c(Name='org_billing_state__c',Salesforce_Field__c='org_billing_state__c'),
			new CCA_Finance_Api_Fields__c(Name='org_billing_zip',Salesforce_Field__c='org_billing_zip__c'),
			new CCA_Finance_Api_Fields__c(Name='org_billing_country',Salesforce_Field__c='org_billing_country__c'),
			new CCA_Finance_Api_Fields__c(Name='send_invoices_via_email',Salesforce_Field__c='send_invoices_via_email__c'),
			new CCA_Finance_Api_Fields__c(Name='organization',Salesforce_Field__c='organization_name__c'),
			new CCA_Finance_Api_Fields__c(Name='organization_ccd_number',Salesforce_Field__c='org_number__c'),
			new CCA_Finance_Api_Fields__c(Name='requested_by',Salesforce_Field__c='requested_by_name__c'),
			new CCA_Finance_Api_Fields__c(Name='requested_date',Salesforce_Field__c='requested_date__c'),
			new CCA_Finance_Api_Fields__c(Name='tax_exempt',Salesforce_Field__c='tax_exempt__c'),
			new CCA_Finance_Api_Fields__c(Name='tax_exempt_number',Salesforce_Field__c='tax_exempt_number__c'),
			new CCA_Finance_Api_Fields__c(Name='po_number_required',Salesforce_Field__c='po_number_required__c'),
			new CCA_Finance_Api_Fields__c(Name='standard_po_number',Salesforce_Field__c='standard_po_number__c'),
			new CCA_Finance_Api_Fields__c(Name='start_date',Salesforce_Field__c='start_date__c'),
			new CCA_Finance_Api_Fields__c(Name='end_date',Salesforce_Field__c='end_date__c')
		}; 

		insert new List<CCA_Contact_Fields__c>{
			new CCA_Contact_Fields__c(Name='Contact_Email__c'),
			new CCA_Contact_Fields__c(Name='Additional_Contact_Email_1__c'),
			new CCA_Contact_Fields__c(Name='Additional_Contact_Email_2__c'),
			new CCA_Contact_Fields__c(Name='Additional_Contact_Email_3__c'),
			new CCA_Contact_Fields__c(Name='Additional_Contact_Email_4__c')
		};
	}

	static testMethod void testBatchJob() {
		setup();

		System.debug('cca1>>' + cca1);
		System.debug('cca2>>' + cca1);
		Test.startTest(); 
		Test.setMock(HttpCalloutMock.class, new WebserviceMock.SyncCCACompareResponse200());
		Database.executeBatch(new SendCCAInfoForCompareBatchable(), 200);      
		Test.stopTest();

		AsyncApexJob batchJobs = [select apexclass.name, TotalJobItems from asyncapexjob where apexclass.name = 'SendCCAInfoForCompareBatchable' LIMIT 1];
		System.assertEquals(1, batchJobs.TotalJobItems, 'Number of batches not as expected');	
	}
	
	static testMethod void testSchedulable() {
		String cronExp = '0 0 0 3 9 ? 2022';
        setup();
        
        Test.startTest();
        Test.setMock(HttpCalloutMock.class, new WebserviceMock.SyncCCACompareResponse500());

        String jobId = System.schedule('testSendccaInfoForCompareScheduable', cronExp, new SendCCAInfoForCompareSchedulable());

        CronTrigger ct = [SELECT Id, CronExpression, TimesTriggered, NextFireTime FROM CronTrigger WHERE id = :jobId];

        // Verify the expressions are the same
    
        System.assertEquals(cronExp, ct.CronExpression);

        // Verify the job has not run 
    
        System.assertEquals(0, ct.TimesTriggered);

        // Verify the next time the job will run 
    
        System.assertEquals('2022-09-03 00:00:00', String.valueOf(ct.NextFireTime));
        Test.stopTest();
	}
	
}