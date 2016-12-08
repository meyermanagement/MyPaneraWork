@isTest
private class SendAuthCafeInfoForCompare_Test {
	
	static CCA_Organization__c cca;
	static Authorized_Cafe__c authCafe;
	static B_C__c cafe;

	static void setup() {
		Organization__c org = TestServices.getOrganization('Test');
		insert org;

		cca = TestServices.getCCAOrganization(org.id, 1, 'Active');
		cafe = new B_C__c(Name='600001',Caf_Number__c='600001');
		insert new List<SObject>{cca,cafe};

		authCafe = TestServices.getAuthorizedCafe(cca.id,cafe.id);
		authCafe.Status__c = 'Active';
		authCafe.Start_Date__c = System.today();
		insert authCafe;

		Remote_Site__c rs = new Remote_Site__c(Send_Authorized_Cafe_for_Compare_Url__c='http://www.foo1.bar/');
		insert rs;

		insert new List<Authorized_Cafe_Finance_Api_Fields__c>{
			new Authorized_Cafe_Finance_Api_Fields__c(Name='Id',Salesforce_Field__c='Id'),
			new Authorized_Cafe_Finance_Api_Fields__c(Name='cafe_oracle_Id',Salesforce_Field__c='cafe_oracle_Id__c'),
			new Authorized_Cafe_Finance_Api_Fields__c(Name='cca_number',Salesforce_Field__c='CCA_Number__c'),
			new Authorized_Cafe_Finance_Api_Fields__c(Name='cafe_number',Salesforce_Field__c='cafe_number__c'),
			new Authorized_Cafe_Finance_Api_Fields__c(Name='cca_organization_name',Salesforce_Field__c='cca_organization_name__c'),
			new Authorized_Cafe_Finance_Api_Fields__c(Name='status',Salesforce_Field__c='Status__c')
		};
	}

	static testMethod void testBatchJob() {
		setup();

		Test.startTest(); 
		Test.setMock(HttpCalloutMock.class, new WebserviceMock.SyncAuthCafeCompareResponse200());
		Database.executeBatch(new SendAuthCafeInfoForCompareBatchable(), 200);      
		Test.stopTest();

		AsyncApexJob batchJobs = [select apexclass.name, TotalJobItems from asyncapexjob where apexclass.name = 'SendAuthCafeInfoForCompareBatchable' LIMIT 1];
		System.assertEquals(1, batchJobs.TotalJobItems, 'Number of batches not as expected');	
	}
	
	static testMethod void testSchedulable() {
		String cronExp = '0 0 0 3 9 ? 2022';
        setup();
        
        Test.startTest();
        Test.setMock(HttpCalloutMock.class, new WebserviceMock.SyncAuthCafeCompareResponse500());

        String jobId = System.schedule('testSendAuthCafeInfoForCompareScheduable', cronExp, new SendAuthCafeInfoForCompareSchedulable());

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