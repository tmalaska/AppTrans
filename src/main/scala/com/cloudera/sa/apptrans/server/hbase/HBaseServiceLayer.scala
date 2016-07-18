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
  @Path("account/{accountId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccount (@PathParam("accountId") accountId:String,
                          @QueryParam("startTime") startTime:String = Long.MaxValue.toString,
                          @QueryParam("endTime")  endTime:String = Long.MinValue.toString): List[AppEvent] = {

    val table = HBaseGlobalValues.connection.getTable(TableName.valueOf(HBaseGlobalValues.accountTableName))

    val scan = new Scan()
    scan.setStartRow(AppEventHBaseHelper.generateRowKey(accountId, startTime.toLong, HBaseGlobalValues.numberOfSalts))
    scan.setStopRow(AppEventHBaseHelper.generateRowKey(accountId, endTime.toLong, HBaseGlobalValues.numberOfSalts))


    val scannerIt = table.getScanner(scan).iterator()

    val appEventList = new mutable.MutableList[AppEvent]

    while(scannerIt.hasNext) {
      val result = scannerIt.next()
      appEventList += AppEventHBaseHelper.convertToAppEvent(result)
    }

    appEventList.toList
  }

  @GET
  @Path("account/ts/{accountId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccountLineage (@PathParam("accountId") accountId:String,
                          @QueryParam("startTime") startTime:String = Long.MaxValue.toString,
                          @QueryParam("endTime")  endTime:String = Long.MinValue.toString): List[AppEvent] = {

    val table = HBaseGlobalValues.connection.getTable(TableName.valueOf(HBaseGlobalValues.accountTableName))

    val scan = new Scan()
    scan.setStartRow(AppEventHBaseHelper.generateRowKey(accountId, startTime.toLong, HBaseGlobalValues.numberOfSalts))
    scan.setStopRow(AppEventHBaseHelper.generateRowKey(accountId, endTime.toLong, HBaseGlobalValues.numberOfSalts))
    val scannerIt = table.getScanner(scan).iterator()

    val appEventList = new mutable.MutableList[AppEvent]

    while(scannerIt.hasNext) {
      val result = scannerIt.next()
      appEventList += AppEventHBaseHelper.convertToAppEvent(result)
    }

    appEventList.toList
  }

}
