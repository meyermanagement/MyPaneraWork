import com.paneraSF.unmarshalledObjects.MergeUnmarshalled
import com.paneraSF.utils.CommonUtils
import org.apache.commons.logging.LogFactory

import static com.paneraSF.CronTriggerConfigService.getCronTrigger
import static com.paneraSF.utils.CommonUtils.*

/**
 * @bmeyers
 * This job checks for errors in merge processing and updates the status in Salesforce
 */
class MergeStatusIntegrationJob {

    // This is to avoid concurrent threads
    def concurrent = false

    static triggers = getCronTrigger("mergeStatusIntegrationTrigger")

    private static final String filesPath = getConfigProperty("cron.dataIntegration.filesPath")
    private static final String mergeStatusLastRunDateFileName = getConfigProperty("cron.dataIntegration.mergeStatus.lastRunDateFileName")
    private static final String mergeStatusLastRunDateFile = "${filesPath}${mergeStatusLastRunDateFileName}"

    private static final log = LogFactory.getLog(this)

    def salesForceService
    def salesforceSOAPService
    def errorHandler
    def groovySqlSalesDataAccess
    def edwService

    Date lastRunDate
    Date updatedLastRunDate

    def execute() {
        log.info("Merge Status Integration Job Starting")
        try {
            salesForceService.initAccessTokenAndUrlInstance()
            salesforceSOAPService.initConnection()
            lastRunDate = CommonUtils.getLastRunDate(mergeStatusLastRunDateFile)
            updatedLastRunDate = groovySqlSalesDataAccess.getServerTimestamp()

            def mergeResultsEDW = edwService.getUnmarshalledUpdatedMergeStatuses(lastRunDate)
            salesforceSOAPService.upsertMergeStatuses(mergeResultsEDW)

            CommonUtils.updateLastRunDate(mergeStatusLastRunDateFile, updatedLastRunDate)
        }  catch (Exception e) {
            CommonUtils.updateLastRunDate(mergeStatusLastRunDateFile, lastRunDate)
            log.error(errorHandler.getErrorMessage(e),e)
        }
    }
}
