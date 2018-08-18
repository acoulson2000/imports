package org.oplontis

import org.codehaus.groovy.grails.web.json.parser.JSONParser

import grails.converters.JSON
import grails.plugins.imports.*
import grails.plugins.imports.logging.DefaultLogger;
import grails.plugins.imports.logging.ImportLogger;
import groovy.sql.Sql

import java.sql.Connection

import javax.sql.DataSource

class MysqlLogger implements ImportLogger {
		
	def createImportLog(params) {
		def doc = DefaultLogger.getLogTemplate(params)
		
		if (params.logValues instanceof Map) {
			doc += params.logValues
		}
		insertDoc(doc)
		return doc._id
	}
	
	def insertDoc(doc) {
		String json = (doc as JSON).toString()
		ImportLog importLog = new ImportLog(document: json)
		importLog.id = doc._id
		importLog.save(flush:true, failOnError: true)
	}
	
	def updateDoc(doc) {
		String json = (doc as JSON).toString()
		ImportLog importLog = ImportLog.get(doc['_id'])		
		def sqlParams = [doc['_id'], json]
		importLog.document = json
		importLog.save(flush:true, failOnError: true)
	}

	def updateImportLog(importLogId, document) {
		updateDoc(document)
		return document._id
	}
	
	def getImportLog(importLogId) {
		ImportLog importLog = ImportLog.get(importLogId)
		def document
		if (importLog != null) {
			document = new JSONParser(new ByteArrayInputStream(importLog['document'].getBytes('UTF-8'))).parse()
		}
		return document
	}
	
    def incrementImportCounter(importLogId) {
		def importLog = getImportLog(importLogId)
		importLog.processed++
		updateImportLog(importLogId, importLog)
    }

    def setImportTotal(importLogId, total) {
		def importLog = getImportLog(importLogId)
		importLog.total = total
		updateImportLog(importLogId, importLog)
    }

    def setImportLogValue(importLogId, name, value) {
		def importLog = getImportLog(importLogId)
		importLog[name] = value
		updateImportLog(importLogId, importLog)
    }

    def cancel(importLogId) {
		def doc = getImportLog(importLogId)
		if (doc?.processing == true) {
			doc.canceled = true
			updateImportLog(importLogId, importLog)
			return true
		} else {
			return false
		}
    }

    def isCanceled(importLogId) {
		def importLog = getImportLog(importLogId)
		return importLog?.canceled
    }

    def isImportComplete(importLogId) {
		def doc = getImportLog(importLogId)
		return doc.total == doc.processed
    }

    def logMessage(importLogId, valuesMap) {
		def importLog = getImportLog(importLogId)
		importLog.messages << valuesMap
		updateImportLog(importLogId, importLog)
    }

    def logSuccessRow(importLogId, row, index) {
		def importLog = getImportLog(importLogId)
		importLog.successCount = importLog.successCount ?: 0i
		importLog.successCount++
		updateImportLog(importLogId, importLog)
    }

    def logCancelRow(importLogId, row, index) {
		def importLog = getImportLog(importLogId)
        importLog.cancelCount = importLog.cancelCount ?: 0i
        importLog.cancelCount++
		updateImportLog(importLogId, importLog)
    }

    def logInsertRow(importLogId, row, index) {
		def importLog = getImportLog(importLogId)
		importLog.insertCount = importLog.insertCount ?: 0i
        importLog.insertCount++
		updateImportLog(importLogId, importLog)
    }

    def logUpdateRow(importLogId, row, index) {
		def importLog = getImportLog(importLogId)
        importLog.updateCount = importLog.updateCount ?: 0i
        importLog.updateCount++
		updateImportLog(importLogId, importLog)
    }

    def logErrorRow(importLogId, row, index, msg) {
		def importLog = getImportLog(importLogId)
        importLog.errorCount = importLog.errorCount ?: 0i
        if (msg) row[DefaultImporter.IMPORT_ERROR] = msg
        if (index) row[DefaultImporter.IMPORT_INDEX] = index
        importLog.errorCount++
        importLog.errorRows << row
		updateImportLog(importLogId, importLog)
    }

    def getImportLogErrorInfo(importLogId) {
		def doc = getImportLog(importLogId),
		    headers = doc?.headers,
		    errorRows = doc?.errorRows,
		    rtn = [errorRows:[]]
		if (headers) {
            if (!headers.contains(DefaultImporter.IMPORT_INDEX)) headers << DefaultImporter.IMPORT_INDEX
            if (!headers.contains(DefaultImporter.IMPORT_ERROR)) headers << DefaultImporter.IMPORT_ERROR
			rtn.errorRows << headers
			if (errorRows) {
				errorRows.each {row->
					def outputRow = []
					headers.each {hdr-> outputRow << row[hdr] }
					rtn.errorRows << outputRow
				}
			}	
			rtn.fileName = 'ERRORS_'+doc.fileName
			rtn.errorCount = doc.errorCount
			return rtn
		} else {
			return null
		}
    }

    def findImportLogs(params) {
		def rslt = ImportLog.listOrderByDateCreated(order: "desc")
		return rslt
    }

}
