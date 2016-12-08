@isTest
private class SendAuthUserInfoForCompare_Test {
	
	static CCA_Organization__c cca;
	static Authorized_user__c authuser;

	static void setup() {
		Organization__c org = TestServices.getOrganization('Test');
		insert org;

		cca = TestServices.getCCAOrganization(org.id, 1, 'Active');
		Lead l = TestServices.getLead(1, LeadServices.qualifiedLeadRT.id);
		insert new List<SObject>{cca,l};

		authUser = TestServices.getAuthorizedUser(cca.id,l.id,null);
		authUser.Status__c = 'Inactive';
		authUser.User_Oracle_Id__c = '00005';
		insert authUser;

		Remote_Site__c rs = new Remote_Site__c(Send_Authorized_User_for_Compare_Url__c='http://www.foo1.bar/', Sync_Auth_User_With_Hub_Url__c='http://www.foo1.bar/');
		insert rs;

		insert new List<Authorized_User_Finance_Api_Fields__c>{
			new Authorized_User_Finance_Api_Fields__c(Name='Id',Salesforce_Field__c='Id'),
			new Authorized_User_Finance_Api_Fields__c(Name='user_oracle_Id',Salesforce_Field__c='user_oracle_Id__c'),
			new Authorized_User_Finance_Api_Fields__c(Name='first_name',Salesforce_Field__c='first_name__c'),
			new Authorized_User_Finance_Api_Fields__c(Name='last_name',Salesforce_Field__c='last_name__c'),
			new Authorized_User_Finance_Api_Fields__c(Name='cca_number',Salesforce_Field__c='cca_number__c'),
			new Authorized_User_Finance_Api_Fields__c(Name='account_ccd_number',Salesforce_Field__c='CCD_Account_Number__c'),
			new Authorized_User_Finance_Api_Fields__c(Name='status',Salesforce_Field__c='status__c')
		};
	}

	static testMethod void testBatchJob() {
		setup();

		Test.startTest(); 
		Test.setMock(HttpCalloutMock.class, new WebserviceMock.SyncAuthUserCompareResponse200());
		Database.executeBatch(new SendAuthUserInfoForCompareBatchable(), 200);      
		Test.stopTest();

		AsyncApexJob batchJobs = [select apexclass.name, TotalJobItems from asyncapexjob where apexclass.name = 'SendAuthUserInfoForCompareBatchable' LIMIT 1];
		System.assertEquals(1, batchJobs.TotalJobItems, 'Number of batches not as expected');	
	}
	
	static testMethod void testSchedulable() {
		String cronExp = '0 0 0 3 9 ? 2022';
        setup();
        
        Test.startTest();
        Test.setMock(HttpCalloutMock.class, new WebserviceMock.SyncAuthUserCompareResponse500());

        String jobId = System.schedule('testSendAuthUserInfoForCompareScheduable', cronExp, new SendAuthUserInfoForCompareSchedulable());

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