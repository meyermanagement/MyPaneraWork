import com.paneraSF.unmarshalledObjects.SaleUnmarshalled
import com.paneraSF.utils.CommonUtils
import org.apache.commons.logging.LogFactory

import static com.paneraSF.CronTriggerConfigService.getCronTrigger

/**
 * This is the cron which runs every day and integrates the Sales between Panera and Salesforce
 *
 */
class Cat30MemberNumberIntegrationJob {

    // This is to avoid concurrent threads
    def concurrent = false

    static triggers = getCronTrigger("cat30MemberNumberIntegrationTrigger")

    private static final String filesPath = CommonUtils.getConfigProperty("cron.dataIntegration.filesPath")
    private static final String lastRunDateFileName = CommonUtils.getConfigProperty("cron.dataIntegration.cat30.lastRunDateFileName")
    private static final String previousLastRunDateFileName = CommonUtils.getConfigProperty("cron.dataIntegration.cat30.previousLastRunDateFileName")
    private static final String lastRunDateFile = "${filesPath}${lastRunDateFileName}"
    private static final String previousLastRunDateFile = "${filesPath}${previousLastRunDateFileName}"

    private static final log = LogFactory.getLog(this)

    def salesForceService
    def salesforceSOAPService
    def edwService
    def groovySqlSalesDataAccess
    def errorHandler

    Date lastRunDate
    Date updatedLastRunDate
    Date previousLastRunDate

    def execute() {
        try {
            salesForceService.initAccessTokenAndUrlInstance()
            salesforceSOAPService.initConnection()
            lastRunDate = CommonUtils.getLastRunDate(lastRunDateFile)
            previousLastRunDate = CommonUtils.getLastRunDate(previousLastRunDateFile)
            /**
             * Retrieve Catering 3.0 sales and associated member numbers from EDW
             * We need to retrieve last run date at the beginning of the job so as not to miss any sales
             *   that are created or updated while the job is running
             */
            updatedLastRunDate = groovySqlSalesDataAccess.getServerTimestamp()
            def cat30SalesEDW = edwService.getCat30Sales(lastRunDate)
            def saleErrorsSF = CommonUtils.getSaleErrors()
            cat30SalesEDW.addAll(CommonUtils.unmarshallSaleErrors(saleErrorsSF))
            /**
             * Upsert Member Numbers to associated sales in Salesforce
             */
            salesforceSOAPService.upsertSales(cat30SalesEDW, saleErrorsSF)
            //Retry any new sale errors with smaller batch size
            def newSaleErrorsSF = CommonUtils.getSaleErrors()
            salesforceSOAPService.upsertSales(CommonUtils.unmarshallSaleErrors(newSaleErrorsSF), newSaleErrorsSF, 20)

            /**
             * Modify sales in EDW with new CCD Account number
             */
            cat30SalesEDW.each { SaleUnmarshalled sale ->
                edwService.modifyAccountOnCat30Sale(sale)
            }
            log.info("Modify Catering 3.0 Sales complete")

            //Only update the last run date if the job makes it to the end
            CommonUtils.updateLastRunDate(lastRunDateFile, updatedLastRunDate)
            CommonUtils.updateLastRunDate(previousLastRunDateFile, lastRunDate)
        } catch (Exception e) {
            //If there is an error, set it back to the original last run date
            CommonUtils.updateLastRunDate(lastRunDateFile, lastRunDate)
            CommonUtils.updateLastRunDate(previousLastRunDateFile, previousLastRunDate)
            log.error(errorHandler.getErrorMessage(e), e)
        }
    }

}

