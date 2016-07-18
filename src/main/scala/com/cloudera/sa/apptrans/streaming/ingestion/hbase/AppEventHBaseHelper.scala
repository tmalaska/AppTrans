package com.cloudera.sa.apptrans.streaming.ingestion.hbase

import com.cloudera.sa.apptrans.model.AppEvent
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

object AppEventHBaseHelper {

  val columnFamily = Bytes.toBytes("f")

  val purchaseCol = Bytes.toBytes("p")
  val paymentTypeCol = Bytes.toBytes("pt")
  val sessionIdCol = Bytes.toBytes("s")
  val latitudeCol = Bytes.toBytes("lt")
  val longitudeCol = Bytes.toBytes("lg")

  def generateRowKey(appEvent: AppEvent, numOfSalts:Int): Array[Byte] = {
    val salt = StringUtils.leftPad(
      Math.abs(appEvent.accountId.hashCode % numOfSalts).toString, 4, "0")

    Bytes.toBytes(salt + ":" +
      appEvent.accountId + ":" +
      appEvent.appId + ":" +
      StringUtils.leftPad(appEvent.eventTimestamp.toString, 11, "0") + ":" +
      appEvent.eventType + ":" +
      appEvent.eventId)
  }

  def generateRowKey(customerId:String, timeStamp:Long, numOfSalts:Int): Array[Byte] = {
    val salt = StringUtils.leftPad(
      Math.abs(customerId.hashCode % numOfSalts).toString, 4, "0")

    Bytes.toBytes(salt + ":" +
      customerId + ":" +
      StringUtils.leftPad(timeStamp.toString, 11, "0") + ":")
  }

  def generatePut(appEvent: AppEvent, numOfSalts:Int): Put = {
    val put = new Put(generateRowKey(appEvent, numOfSalts))
    put.addColumn(columnFamily, purchaseCol, Bytes.toBytes(appEvent.purchase.toString))
    put.addColumn(columnFamily, paymentTypeCol, Bytes.toBytes(appEvent.paymentType))
    put.addColumn(columnFamily, sessionIdCol, Bytes.toBytes(appEvent.sessionId))
    put.addColumn(columnFamily, latitudeCol, Bytes.toBytes(appEvent.latitude.toString))
    put.addColumn(columnFamily, longitudeCol, Bytes.toBytes(appEvent.longitude.toString))
    put
  }

  def convertToAppEvent(result:Result): AppEvent = {
    val keyParts = Bytes.toString(result.getRow).split(":")
    val accountId = keyParts(1)
    val appId = keyParts(2)
    val eventTimestamp = keyParts(3).toLong
    val eventType = keyParts(4)
    val eventId = keyParts(5)

    if (eventType.equals("trans")) {
      val transId = keyParts(4)
      val purchase = getResultString(result, purchaseCol).toDouble
      val paymentType = getResultString(result, paymentTypeCol)
      val sessionId = getResultString(result, sessionIdCol)
      val latitude = getResultString(result, latitudeCol).toDouble
      val longitude = getResultString(result, longitudeCol).toDouble

      new AppEvent(accountId,
        appId,
        eventTimestamp,
        eventId,
        eventType,
        purchase,
        paymentType,
        sessionId,
        latitude,
        longitude)
    } else {
      null
    }
  }

  private def getResultString(result:Result, column:Array[Byte]): String = {
    val cell = result.getColumnLatestCell(columnFamily, column)
    Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }
}
