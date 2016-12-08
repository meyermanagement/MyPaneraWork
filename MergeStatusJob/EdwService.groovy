package com.paneraSF

import com.paneraSF.domain.SaleReassignment
import com.paneraSF.enumerator.ErrorType
import com.paneraSF.enumerator.From
import com.paneraSF.handler.ErrorHandler
import com.paneraSF.unmarshalledObjects.MergeUnmarshalled
import com.paneraSF.unmarshalledObjects.ModifySaleUnmarshalled
import com.paneraSF.unmarshalledObjects.SaleUnmarshalled

import static com.paneraSF.utils.CommonUtils.pluralize

/**
 * @bmeyers
 */
class EdwService {

    def salesDataAccess
    def cafeDataAccess
    ErrorHandler errorHandler


    def getServerTimestamp = {
        return salesDataAccess.getServerTimestamp();
    }

    /**
     * Gets all Sales created or updated after specified date
     */
    def getUnmarshalledSales = { date ->
        log.info("Going to retrieve sales created or updated since ${date}")
        return salesDataAccess.getSalesByUpdatedDate(date)
    }

    def getCat30Sales = { date ->
        log.info("Going to retrieve Catering 3.0 sales created or updated since ${date}")
        return salesDataAccess.getCat30SalesByUpdatedDate(date)
    }

    def getSalesCount = { date ->
        log.info("Going to count sales created since ${date}")
        return salesDataAccess.countSalesByInsertDate(date)
    }

    /**
     * Retrieve sales updated by dcr process
     */
    def getDCRSalesToProcess = {
        log.info("Going to retrieve sales that have been updated by manual DCR process")
        def salesToProcess = salesDataAccess.getDCRSalesToProcess()
        log.info("Retrieved ${salesToProcess.size()} ${pluralize(salesToProcess.size(), 'sale', 'sales')} to process")
        return salesToProcess
    }

    /**
     * Mark sales in dcr table as processed by salesforce
     */
    def setDCRSalesAsProcessed = { Collection<SaleReassignment> processedSales ->
        def updateCount = salesDataAccess.setDCRSalesAsProcessed(processedSales)
        log.info("Marked ${updateCount} ${pluralize(updateCount, 'sale', 'sales')} as processed")
    }

    /**
     * Retrieve sale by dw id
     */
    def getUnmarshalledSale = { String saleDwId ->
        log.info("Going to retrieve sale from edw: saleDwId->${saleDwId}")
        def result = salesDataAccess.getSaleByDwId(saleDwId)
        return result && result.size() > 0 ? result.get(0) : null
    }

    def getUnmarshalledDiscounts = { dateStart, dateEnd, discountCodes ->
        log.info("Going to retrieve discounts created or updated since ${dateStart} until ${dateEnd}")
        return salesDataAccess.getDiscountsByUpdatedDate(dateStart, dateEnd, discountCodes)
    }
    /**
     * Insert record into merge staging table of EDW
     */
    def addMergeRequest = { MergeUnmarshalled merge, List mergeErrors ->
        def mergeError = mergeErrors.find { error -> error.name == merge.name }
        if (mergeError) mergeErrors.remove(mergeError)
        String edwResponse = salesDataAccess.addMergeRequest(merge.mergedCCD, merge.survivorCCD, merge.name)
        log.info("edwResponse -> ${edwResponse}")
        if (edwResponse != 'Success') {
            errorHandler.storeMergeError(merge,mergeError,"100","Error inserting merge into EDW -> ${edwResponse}",From.EDW, ErrorType.RECOVERABLE,true)
            log.error("SF Error Inserting SF-Merge-id ${merge.name} into EDW staging table")
        }
    }

    /**
     * Calls EDW package that will run merge for sale records in EDW
     */
    def processMergeRequests = {
        String edwResponse = salesDataAccess.processMergeRequests()
        return edwResponse
    }

    /**
     * Retrieves updated edw status of merge requests
     */
    def getUnmarshalledUpdatedMergeStatuses = { date ->
        log.info("Going to retrieve status of merge requests submitted or updated since ${date}")
        return salesDataAccess.getMergeStatusByUpdatedDate(date)
    }

    /**
     * Modify customer account number on sale in EDW
     */
    def modifyAccountOnSale = { ModifySaleUnmarshalled modifiedSale ->
        return salesDataAccess.modifyAccountOnSaleEdw(modifiedSale)
    }

    def modifyAccountOnCat30Sale = { SaleUnmarshalled sale ->
        log.info("Going to update sale in edw: CCD Sale Number->${sale.ccdSaleNumber}")
        return salesDataAccess.modifyAccountOnCat30SaleEdw(sale)
    }

    /**
     * Get cafe data from EDW
     */
    def getCafeData = {
        return cafeDataAccess.getCafeData()
    }

    /**
     * Get cafes from EDW with status of "Dead" or "Dead Cafe"
     */
    def getDeadCafes = {
        return cafeDataAccess.getDeadCafes()
    }
}
