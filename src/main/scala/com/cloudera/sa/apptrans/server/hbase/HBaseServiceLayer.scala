package com.cloudera.sa.apptrans.server.hbase

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import com.cloudera.sa.apptrans.model.{AppEvent, AppEventBuilder}
import com.cloudera.sa.apptrans.streaming.ingestion.hbase.{AppEventHBaseHelper}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}

import scala.collection.mutable

@Path("rest")
class HBaseServiceLayer {

  @GET
  @Path("hello")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def hello(): String = {
    "Hello World"
  }

  @GET
  @Path("appEvent/{accountId}/{appId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccount (@PathParam("accountId") accountId:String,
                  @PathParam("appId") appId:String,
                          @QueryParam("startTime") startTime:String = Long.MaxValue.toString,
                          @QueryParam("endTime")  endTime:String = Long.MinValue.toString): List[AppEvent] = {

    val table = HBaseGlobalValues.connection.getTable(TableName.valueOf(HBaseGlobalValues.appEventTableName))

    val scan = new Scan()
    scan.setStartRow(AppEventHBaseHelper.generateRowKey(accountId, appId, startTime.toLong, HBaseGlobalValues.numberOfSalts))
    scan.setStopRow(AppEventHBaseHelper.generateRowKey(accountId, appId, endTime.toLong, HBaseGlobalValues.numberOfSalts))


    val scannerIt = table.getScanner(scan).iterator()

    val appEventList = new mutable.MutableList[AppEvent]

    while(scannerIt.hasNext) {
      val result = scannerIt.next()
      appEventList += AppEventHBaseHelper.convertToAppEvent(result)
    }

    appEventList.toList
  }

  @GET
  @Path("accountMart/{accountId}/{appId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccountLineage (@PathParam("accountId") accountId:String,
                         @PathParam("appId") appId:String): List[AppEvent] = {

    val table = HBaseGlobalValues.connection.getTable(TableName.valueOf(HBaseGlobalValues.accountMartTableName))

    val scan = new Scan()
    scan.setStartRow(AppEventHBaseHelper.generateRowKey(accountId, appId, HBaseGlobalValues.numberOfSalts))
    scan.setStopRow(AppEventHBaseHelper.generateRowKey(accountId, appId, HBaseGlobalValues.numberOfSalts))
    val scannerIt = table.getScanner(scan).iterator()

    val appEventList = new mutable.MutableList[AppEvent]

    while(scannerIt.hasNext) {
      val result = scannerIt.next()
      appEventList += AppEventHBaseHelper.convertToAppEvent(result)
    }

    appEventList.toList
  }

}
