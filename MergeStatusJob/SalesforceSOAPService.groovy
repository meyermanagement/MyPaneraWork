package com.paneraSF

import com.paneraSF.domain.*
import com.paneraSF.enumerator.ClientType
import com.paneraSF.enumerator.ErrorType
import com.paneraSF.enumerator.From
import com.paneraSF.enumerator.SObjectType
import com.paneraSF.unmarshalledObjects.*
import com.paneraSF.utils.CommonUtils
import com.sforce.soap.partner.GetServerTimestampResult
import com.sforce.soap.partner.PartnerConnection
import com.sforce.soap.partner.fault.UnexpectedErrorFault
import com.sforce.soap.partner.sobject.SObject
import com.sforce.ws.ConnectionException
import com.sforce.ws.ConnectorConfig

import java.lang.reflect.UndeclaredThrowableException

import static com.paneraSF.utils.CommonUtils.*

class SalesforceSOAPService {
    final String salesForceSoapApiUrl = CommonUtils.getConfigProperty("salesForce.connectionSettings.soap.url")
    final String username = CommonUtils.getConfigProperty("salesForce.connectionSettings.username")
    final String password = CommonUtils.getConfigProperty("salesForce.connectionSettings.password")
    final Integer maxBatchSize = CommonUtils.getConfigProperty("cron.dataIntegration.salesforceSOAP.maxBatchSize")
    final String cafeExternalIdFieldName = CommonUtils.getConfigProperty("cron.dataIntegration.cafeExternalIdFieldName")
    final Integer maxSFWhereClauseBatchSize = CommonUtils.getConfigProperty("cron.dataIntegration.maxSFWhereClauseBatchSize")
    final Integer maxRequestRetries = 3

    def salesForceService
    def saleMarshaller
    def cafeMarshaller
    def clientMarshaller
    def addressMarshaller
    def bonusPeriodMarshaller
    def discountMarshaller
    def contactHistoryMarshaller
    def mergeMarshaller
    def errorHandler

    def connection

    /**
     * Connects to SalesForce Soap Api if not already connected
     */
    def initConnection = {
        if (!connection) {
            log.info("SF Init Connection")
            connectToSoapApi()
        }
    }

    /**
     * 	Connects to SFDC and refreshes the connection
     */
    def refreshConnection = {
        log.info("SF Refresh Connection")
        connectToSoapApi()
    }

    /**
     * 	Connects to SFDC and refreshes the connection, retries 3 times if unexpected error
     */
    def connectToSoapApi() {
        def retryConnection=true; def retryCount=0
        while (retryConnection && retryCount<=maxRequestRetries) {
            retryConnection = false
            try {
                ConnectorConfig partnerConfig = new ConnectorConfig()
                partnerConfig.setUsername(username)
                partnerConfig.setPassword(password)
                partnerConfig.setAuthEndpoint(salesForceSoapApiUrl)
                connection = new PartnerConnection(partnerConfig)
                def config = connection.getConfig()
                log.info("Session ID = ${config.getSessionId()} and Instance Url = ${config.getServiceEndpoint()?.split('/services')[0]}")
            } catch (ConnectionException e) {
                def errorMessage = errorHandler.getErrorMessage(e)
                log.error(errorMessage)
                retryConnection=shouldRetry(e); retryCount++
            } finally {
                if (retryConnection && retryCount<=maxRequestRetries) {
                    log.error("Retrying connection, retries->$retryCount")
                } else if (!connection) {
                    throw new Exception ("Unable to connect to Salesforce SOAP API")
                }
            }
        }
    }

    def shouldRetry(ConnectionException e) {
        return e.class == UndeclaredThrowableException || e.class == UnexpectedErrorFault
    }

    /**
     * Connects to SalesForce Soap Api and returns the server time
     */
    def Date getServerDate() {
        GetServerTimestampResult result = executeRequest { connection.getServerTimestamp()}
        Date theServerDate = result.getTimestamp().getTime()
        log.info("Retrieved SF server time: ${theServerDate}.")
        return theServerDate
    }

    /**
     * 	Executes a request in SFDC. If session expired refreshes it and executes request again
     */
    def executeRequest(Closure requestClosure) {
        initConnection()
        def resp
        def retryRequest=true; def retryCount=0
        while (retryRequest && retryCount<=maxRequestRetries) {
            retryRequest = false
            try {
                resp = requestClosure.call()
                if (salesForceService.sessionExpired(resp)) {
                    refreshConnection()
                    retryRequest=true; retryCount++
                }
            } catch (ConnectionException e) {
                retryRequest=shouldRetry(e); retryCount++
                if (!retryRequest || retryCount>maxRequestRetries) {
                    //Method that initiated request should handle exception
                    throw e
                } else {
                    def errorMessage = errorHandler.getErrorMessage(e)
                    log.error(errorMessage)
                }
            } finally {
                if (retryRequest && retryCount<=maxRequestRetries) {
                    log.error("Retrying SOAP Request, retries->$retryCount")
                }
            }
        }
        return resp
    }

    SObject[] createSObjectArray(Collection sobjects) {
        return sobjects.toArray(new SObject[sobjects.size()])
    }

    def upsertSObjects(Collection<SObject> recordsToUpsert, String externalIdFieldName) {
        return executeRequest { connection.upsert(externalIdFieldName, createSObjectArray(recordsToUpsert)) }
    }

    def createSObjects(Collection<SObject> recordsToCreate) {
        return executeRequest { connection.create(createSObjectArray(recordsToCreate)) }
    }

    def updateSObjects(Collection<SObject> recordsToUpdate){
        return executeRequest { connection.update(createSObjectArray(recordsToUpdate)) }
    }

    def deleteSObjects(Collection<SObject> recordsToDelete) {
        def idsToDelete = recordsToDelete.collect { it.getField("Id") }
        return executeRequest { connection.delete(idsToDelete as String[])}
    }

    /**
     * Unchecks wasUpdatedOrCreatedInSf flag for list of clients using the SOAP API
     */
    def uncheckWasUpdatedInSfFlag(List<Client> clients) {
        def sobjects = clients.collect { client ->
            def type = client.clientType == ClientType.CLIENT ? "Account" : "Lead"
            createSObject(client.salesForceId, type, ["wasUpdatedOrCreatedInSf__c": false], null)
        }
        CommonUtils.createBatches(sobjects, maxBatchSize).each { batch ->
            try {
                updateSObjects(batch)
            } catch (Throwable e) {
                def errorMessage = errorHandler.getErrorMessage(e)
                log.error(errorMessage,e)
            }

        }
    }
    /**
     * Insert activity data using the SOAP API
     */
    def upsertActivities(def activities, def errors, def batchSize=50){
        log.info("Number of activities to upsert: ${activities.size()}")
        def taskBatches = CommonUtils.createBatches(activities.findAll{ContactHistory ch -> ch.callType.getSobjectType() == SObjectType.TASK}, batchSize)
        def eventBatches = CommonUtils.createBatches(activities.findAll{ContactHistory ch -> ch.callType.getSobjectType() == SObjectType.EVENT}, batchSize)
        def totalBatchSize = taskBatches.size() + eventBatches.size()
        log.info("Upserting activities. Number of batches: ${totalBatchSize}")
        def batchCount = 1
        taskBatches.each { batch ->
            log.info("Processing batch ${batchCount++} of ${totalBatchSize}. Batch size: ${batch.size()}")
            def results = upsertSObjects(contactHistoryMarshaller.marshallSalesForceSObjects(batch), "Customer_Hub_Activity_Id__c")
            handleActivityResponse(results, batch, errors)
        }
        eventBatches.each { batch ->
            log.info("Processing batch ${batchCount++} of ${totalBatchSize}. Batch size: ${batch.size()}")
            def results = upsertSObjects(contactHistoryMarshaller.marshallSalesForceSObjects(batch), "Customer_Hub_Activity_Id__c")
            handleActivityResponse(results, batch, errors)
        }
        log.info("Finished upserting activities")
    }

    def upsertClients(Collection<ClientUnmarshalled> clientsUnmarshalled, Collection<ClientError> clientErrors) {
        clientsUnmarshalled?.each { clientUnmarshalled ->
            ClientError clientError = clientErrors.find { error -> error.ccdAccountNumber == clientUnmarshalled.ccdAccountNumber }
            try {
                log.info("SF Upserting Client: ccd-id-> ${clientUnmarshalled.ccdAccountNumber} first-name-> ${clientUnmarshalled.firstName} last-name-> ${clientUnmarshalled.lastName}")
                log.debug("SF Upserting ${clientUnmarshalled.addresses?.size()}"+" ${pluralize(clientUnmarshalled.addresses?.size(),'address', 'addresses')}")
                log.debug("SF client is-> ${getObjectProperties(clientUnmarshalled)}")
                log.debug("SF client addresses is-> ${clientUnmarshalled?.addresses?.collect { getObjectProperties(it)} }")
                def sobjsToCreate = []
                def sobjsToUpdate = []
                def sobjsToDelete = []

                if (clientUnmarshalled.salesForceId == null) {
                    sobjsToCreate.addAll(clientMarshaller.marshallSalesForceSObjects([clientUnmarshalled]))
                } else if (clientUnmarshalled.salesForceId != null) {
                    sobjsToUpdate.addAll(clientMarshaller.marshallSalesForceSObjects([clientUnmarshalled]))
                }

                sobjsToCreate.addAll(addressMarshaller.marshallSalesForceSObjects(clientUnmarshalled.addresses.findAll{!it.isDelete && it.salesForceId == null}))
                sobjsToUpdate.addAll(addressMarshaller.marshallSalesForceSObjects(clientUnmarshalled.addresses.findAll{!it.isDelete && it.salesForceId != null}))
                sobjsToDelete.addAll(addressMarshaller.marshallSalesForceSObjects(clientUnmarshalled.addresses.findAll{it.isDelete}))
                log.debug("sobjsToCreate: ${sobjsToCreate}")
                log.debug("sobjsToUpdate: ${sobjsToUpdate}")
                log.debug("sobjsToDelete: ${sobjsToDelete}")

                if (!sobjsToCreate.isEmpty()) {
                    def results = createSObjects(sobjsToCreate)
                    log.debug("Create results: ${results}")
                    handleClientResponse(results, sobjsToCreate, clientUnmarshalled, clientError)
                }
                if (!sobjsToUpdate.isEmpty()) {
                    def results = updateSObjects(sobjsToUpdate)
                    log.debug("Update results: ${results}")
                    handleClientResponse(results, sobjsToUpdate, clientUnmarshalled, clientError)
                }
                if (!sobjsToDelete.isEmpty()) {
                    def results = deleteSObjects(sobjsToDelete)
                    log.debug("Delete results: ${results}")
                    handleClientResponse(results, sobjsToDelete, clientUnmarshalled, clientError)
                }

            } catch (Throwable e) {
                salesForceService.handleClientUnexpectedError(clientUnmarshalled, clientError, e)
            }
        }
    }

    /**
     * Upsert sale data using the SOAP API
     */
    def upsertSales(def sales, def errors, def batchSize=50) {
        log.info("Number of sales to upsert: ${sales.size()}")
        def saleBatches = CommonUtils.createBatches(sales, batchSize)
        log.info("Upserting sales. Number of batches: ${saleBatches.size()}")
        def batchCount = 1
        saleBatches.each{ batch ->
            log.info("Processing batch ${batchCount++} of ${saleBatches.size()}. Batch size: ${batch.size()}")
            def results = upsertSObjects(saleMarshaller.marshallSalesForceSObjects(batch), "CCD_Sales_Number__c")
            handleSaleResponse(results, batch, errors)
        }
        log.info("Finished upserting sales")
    }
    /**
     * Upsert cafe data using the SOAP API
     */
    def upsertCafes(def cafes) {
        log.info("Number of cafes to upsert: ${cafes.size()}")
        def cafeBatches = CommonUtils.createBatches(cafes, maxBatchSize)
        log.info("Upserting cafes. Number of batches: ${cafeBatches.size()}")
        cafeBatches.each{ batch ->
            log.info("Processing batch. Batch size: ${batch.size()}")
            def results = upsertSObjects(cafeMarshaller.marshallSalesForceSObjects(batch), cafeExternalIdFieldName)
            handleCafeResponse(results, batch)
        }
        log.info("Finished upserting cafes")
    }

    def upsertMergeStatuses(def merges){
        log.info("Number of merge statuses to upsert: ${merges.size()}")
        def mergeBatches = CommonUtils.createBatches(merges, maxBatchSize)
        log.info("Upserting merge statuses. Number of batches: ${mergeBatches.size()}")
        mergeBatches.each{ batch ->
            log.info("Processing batch. Batch size: ${batch.size()}")
            def results = upsertSObjects(mergeMarshaller.marshallSalesForceSObjectsStatus(batch),"name")
            handleMergeStatusResponse(results, batch)
        }
        log.info("Finished upserting merge statuses")
    }

    /**
     * Delete dead cafes from Salesforce
     */
    def deleteDeadCafes(deadCafes) {
        if(!deadCafes.isEmpty()) {
            def externalIdFieldName = cafeExternalIdFieldName
            //First check which cafes are in Salesforce
            def deadCafeNumbers = deadCafes.collect {CafeUnmarshalled cafe -> cafe.cafeNumber}
            def deadCafeIds = []; def cafeNumbersThisBatch = []; def count = 0
            deadCafeNumbers.each{ cafeNumber ->
                cafeNumbersThisBatch << cafeNumber
                count++
                if(cafeNumbersThisBatch.size() >= maxSFWhereClauseBatchSize || count == deadCafeNumbers.size()) {
                    def query = 'SELECT Id FROM B_C__c where ' + CommonUtils.createSalesforceInWhereClause("Caf_Number__c", cafeNumbersThisBatch)
                    deadCafeIds.addAll(salesForceService.executeQuery(query).records.collect {it."Id"})
                    cafeNumbersThisBatch.clear()
                }
            }
            //If any exist in Salesforce, go and delete them
            if(!deadCafeIds.isEmpty()){
                log.info("Found ${deadCafeIds.size()} dead ${deadCafeIds.size()==1?"cafe":"cafes"} in Salesforce")
                def deadCafeIdsBatches = CommonUtils.createBatches(deadCafeIds, maxBatchSize)
                log.info("Deleting dead cafes. Number of batches: ${deadCafeIdsBatches.size()}")
                deadCafeIdsBatches.each{ batch ->
                    log.info("Processing batch. Batch size: ${batch.size()}")
                    def results = deleteSObjects(batch?. collect {createSObject(it,null,null,null)})
                    handleCafeResponse(results, batch)
                }
            } else {
                log.info("No dead cafes found in Salesforce")
            }
        }
    }

    def deleteBonusChangeLogs = { bonusChangeLogs, def batchSize=50 ->
        log.info("Number of bonus change logs to delete: ${bonusChangeLogs.size()}")
        def bonusChangeLogBatches = CommonUtils.createBatches(bonusChangeLogs, batchSize)
        log.info("Deleting bonus change logs. Number of batches: ${bonusChangeLogBatches.size()}")
        def batchCount = 1
        bonusChangeLogBatches.each{ batch ->
            log.info("Processing batch ${batchCount++} of ${bonusChangeLogBatches.size()}. Batch size: ${batch.size()}")
            def results = deleteSObjects(batch?.collect {createSObject(it.salesforceId,null,null,null)})
            handleBonusChangeLogBatchResponse(results, batch)
        }
        log.info("Finished deleting bonus change logs")
    }

    def updateBonusPeriods = { def bonusPeriods, def batchSize=50 ->
        log.info("Number of bonus periods to update: ${bonusPeriods.size()}")
        def bonusPeriodBatches = CommonUtils.createBatches(bonusPeriods, batchSize)
        log.info("Updating bonus periods. Number of batches: ${bonusPeriodBatches.size()}")
        def batchCount = 1
        bonusPeriodBatches.each{ batch ->
            log.info("Processing batch ${batchCount++} of ${bonusPeriodBatches.size()}. Batch size: ${batch.size()}")
            def results = updateSObjects(bonusPeriodMarshaller.marshallSalesForceSObjects(batch))
            handleBonusPeriodBatchResponse(results, batch)
        }
        log.info("Finished updating bonus periods")
    }

    def upsertDiscountsBatch(Collection<Discount> discounts, Collection<DiscountError> discountErrors, def batchSize=50){
        def externalIdFieldName = "Discount_Id__c"
        log.info("Number of discounts to upsert: ${discounts.size()}")
        def discountBatches = CommonUtils.createBatches(discounts, batchSize)
        log.info("Upserting discounts. Number of batches: ${discountBatches.size()}")
        def batchCount = 1
        discountBatches.each{ batch ->
            log.info("Processing batch ${batchCount++} of ${discountBatches.size()}. Batch size: ${batch.size()}")
            def results = upsertSObjects(discountMarshaller.marshallSalesForceSObjects(batch), externalIdFieldName)
            handleDiscountBatchResponse(results, batch, discountErrors)
        }
        log.info("Finished upserting discounts")
    }

    def handleActivityResponse(results, List<ContactHistoryUnmarshalled> activitiesToProcessThisBatch, List<ContactHistoryError> previousErrors) {
        def countSuccesses=0, countFailures=0, count=0
        results.each { result ->
            ContactHistoryUnmarshalled contactHistory = activitiesToProcessThisBatch[count]
            ContactHistory error = previousErrors.find { previousError -> (previousError.ccdAccountNumber == contactHistory.ccdAccountNumber) && (previousError.activityDate == contactHistory.activityDate) && (previousError.callType == contactHistory.callType) }
            if (result.isSuccess()) {
                countSuccesses++
                if (error) {
                    error.delete(flush:true)
                }
            }
            else {
                countFailures++
                com.sforce.soap.partner.Error[] errors = result.getErrors()
                if (errors.length > 0) {
                    if(contactHistory.class == ContactHistoryUnmarshalled) {
                        errorHandler.storeContactHistoryError(contactHistory, error, errors[0].getStatusCode().toString(), errors[0].getMessage(), From.SF, ErrorType.RECOVERABLE, true)
                        log.error("Error loading contactHistory-> CCD Account Number: ${contactHistory.ccdAccountNumber} Date: ${contactHistory.activityDate} Subject: ${contactHistory.subject}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                    } else {
                        log.error("Error: ${contactHistory}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                    }
                }
            }
            count++
        }
        log.info("Results: ${countSuccesses} succeeded, ${countFailures} failed")
    }

    def handleClientResponse(results, sobjectsToProcess, ClientUnmarshalled client, ClientError previousError) {
        def countSuccesses=0, countFailures=0, count=0, countAddressSuccesses=0
        results?.each { result ->
            SObject sobj = sobjectsToProcess[count]
            log.debug("Sobj is: ${sobj.properties}")
            if (result.isSuccess()) {
                countSuccesses++
                if (sobj.getType() == 'Account' || sobj.getType() == 'Lead'){
                    log.info("SF Upserted Client: ccd-id-> ${client.ccdAccountNumber} first-name-> ${client.firstName} last-name-> ${client.lastName}")
                    client.salesForceId = result.getId()
                    if (previousError) {
                        previousError.delete(flush:true)
                    }
                } else if (sobj.getType() == 'Address__c'){
                    (getClientRelatedUnmarshalledObjFromSObj(sobj, client) as AddressUnmarshalled).salesForceId = result.getId()
                    countAddressSuccesses++
                }
            }
            else {
                countFailures++
                com.sforce.soap.partner.Error[] errors = result.getErrors()
                if (errors.length > 0) {
                    if(sobj.getType() == 'Account' || sobj.getType() == 'Lead') {
                        errorHandler.storeClientError(client, previousError, errors[0].getStatusCode().toString(), errors[0].getMessage(), From.SF, ErrorType.RECOVERABLE, true)
                        log.error("Error loading client->  ccd-id-> ${client.ccdAccountNumber} first-name-> ${client.firstName} last-name-> ${client.lastName}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                    } else if (sobj.getType()  == 'Address__c') {
                        AddressUnmarshalled addressUnmarshalled = getClientRelatedUnmarshalledObjFromSObj(sobj, client) as AddressUnmarshalled
                        log.info("Error processing address for client ccd-id  ${client.ccdAccountNumber} address-ch-id-> ${addressUnmarshalled.customerHubId}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                    }
                }
            }
            count++
        }
        log.debug("SF Upserted ${countAddressSuccesses} addresses")
        log.debug("Results: ${countSuccesses} succeeded, ${countFailures} failed")
    }

    def getClientRelatedUnmarshalledObjFromSObj(SObject sobj, ClientUnmarshalled client) {
        if (sobj.getType() == 'Address__c'){
            return client.addresses.find{it.customerHubId == sobj.getField('Customer_Hub_Address_Id__c')}
        }
    }

    def handleSaleResponse(results, List<SaleUnmarshalled> salesToProcessThisBatch, List<SaleError> previousErrors) {
        def countSuccesses=0, countFailures=0, count=0
        results.each { result ->
            SaleUnmarshalled sale = salesToProcessThisBatch[count]
            SaleError error = previousErrors.find { previousError -> previousError.ccdSaleNumber == sale.ccdSaleNumber }
            if (result.isSuccess()) {
                countSuccesses++
                if (error) {
                    error.delete(flush:true)
                }
            }
            else {
                countFailures++
                com.sforce.soap.partner.Error[] errors = result.getErrors()
                if (errors.length > 0) {
                    if(sale.class == SaleUnmarshalled) {
                        errorHandler.storeSaleError(sale, error, errors[0].getStatusCode().toString(), errors[0].getMessage(), From.SF, ErrorType.RECOVERABLE, true)
                        log.error("Error loading sale nbr ${sale.ccdSaleNumber}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                    } else {
                        log.error("Error: ${sale}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                        //TODO: Handle error
                    }
                }
            }
            count++
        }
        log.info("Results: ${countSuccesses} succeeded, ${countFailures} failed")
    }

    def handleCafeResponse(results, cafesToProcessThisBatch) {
        def countSuccesses=0, countFailures=0, count=0
        results.each { result ->
            if (result.isSuccess()) {
                countSuccesses++
            }
            else {
                countFailures++
                com.sforce.soap.partner.Error[] errors = result.getErrors()
                def cafe = cafesToProcessThisBatch[count]
                if (errors.length > 0) {
                    if(cafe.class == CafeUnmarshalled) {
                        log.error("Error loading cafe nbr ${cafe.cafeNumber}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                        //TODO: Handle error
                    } else {
                        log.error("Error: ${cafe}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                        //TODO: Handle error
                    }
                }

            }
            count++
        }
        log.info("Results: ${countSuccesses} succeeded, ${countFailures} failed")
    }

    def handleBonusChangeLogBatchResponse(results, List<BonusChangeLog> bonusChangeLogsToProcessThisBatch) {
        def countSuccesses=0, countFailures=0, count=0
        results.each { result ->
            BonusChangeLog bonusChangeLog = bonusChangeLogsToProcessThisBatch[count]
            if (result.isSuccess()) {
                countSuccesses++
            }
            else {
                countFailures++
                com.sforce.soap.partner.Error[] errors = result.getErrors()
                if (errors.length > 0) {
                    log.error("Error deleting bonus change log name ${bonusChangeLog.name}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                }
            }
            count++
        }
        log.info("Results: ${countSuccesses} succeeded, ${countFailures} failed")
    }

    def handleBonusPeriodBatchResponse(results, List<BonusPeriodUnmarshalled> periodsToProcessThisBatch) {
        def countSuccesses=0, countFailures=0, count=0
        results.each { result ->
            BonusPeriodUnmarshalled bonusPeriod = periodsToProcessThisBatch[count]
            if (result.isSuccess()) {
                countSuccesses++
            }
            else {
                countFailures++
                com.sforce.soap.partner.Error[] errors = result.getErrors()
                if (errors.length > 0) {
                    if(bonusPeriod.class == BonusPeriodUnmarshalled) {
                        throw new Exception("Error loading bonus period id ${bonusPeriod.salesforceId}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                    } else {
                        throw new Exception("Error: ${bonusPeriod}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                    }
                }
            }
            count++
        }
        log.info("Results: ${countSuccesses} succeeded, ${countFailures} failed")
    }

    def handleDiscountBatchResponse(results, List<DiscountUnmarshalled> discountsToProcessThisBatch, List<DiscountError> previousErrors) {
        def countSuccesses=0, countFailures=0, count=0
        results.each { result ->
            DiscountUnmarshalled discount = discountsToProcessThisBatch[count]
            DiscountError error = previousErrors.find { previousError -> previousError.discountId == discount.discountId }
            if (result.isSuccess()) {
                countSuccesses++
                if (error) {
                    error.delete(flush:true)
                }
            }
            else {
                countFailures++
                com.sforce.soap.partner.Error[] errors = result.getErrors()
                if (errors.length > 0) {
                    if(discount.class == DiscountUnmarshalled) {
                        errorHandler.storeDiscountError(discount, error, errors[0].getStatusCode().toString(), errors[0].getMessage(), From.SF, ErrorType.RECOVERABLE, true)
                        log.error("Error loading discount nbr ${discount.discountId}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                    } else {
                        log.error("Error: ${discount}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                        //TODO: Handle error
                    }
                }
            }
            count++
        }
        log.info("Results: ${countSuccesses} succeeded, ${countFailures} failed")
    }
    def handleMergeStatusResponse(results, mergesToProcessThisBatch) {
        def countSuccesses = 0, countFailures = 0, count = 0
        results.each { result ->
            if (result.isSuccess()) {
                countSuccesses++
            } else {
                countFailures++
                com.sforce.soap.partner.Error[] errors = result.getErrors()
                def merge = mergesToProcessThisBatch[count]
                if (errors.length > 0) {
                    if (merge.class == MergeUnmarshalled) {
                        log.error("Error loading merge nbr ${merge.SFId}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                        //TODO: Handle error
                    } else {
                        log.error("Error: ${merge}, message: ${errors[0].getStatusCode().toString()} ${errors[0].getMessage()}")
                        //TODO: Handle error
                    }
                }

            }
            count++
        }
        log.info("Results: ${countSuccesses} succeeded, ${countFailures} failed")
    }


}
