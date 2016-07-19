package com.cloudera.sa.apptrans.streaming.ingestion.hbase

import com.cloudera.sa.apptrans.model.{AccountMart, AppEvent}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

object AppEventHBaseHelper {

  val columnFamily = Bytes.toBytes("f")

  //AppEvent Columns
  val purchaseCol = Bytes.toBytes("p")
  val paymentTypeCol = Bytes.toBytes("pt")
  val sessionIdCol = Bytes.toBytes("s")
  val latitudeCol = Bytes.toBytes("lt")
  val longitudeCol = Bytes.toBytes("lg")

  //AccountMart Columns
  val signOnCountCol = Bytes.toBytes("s")
  val winCountCol = Bytes.toBytes("w")
  val loseCountCol = Bytes.toBytes("l")
  val purchaseTotalCol = Bytes.toBytes("pt")
  val paymentCreditTotalCol = Bytes.toBytes("pct")
  val paymentCreditCountCol = Bytes.toBytes("pcc")
  val paymentDebitTotalCol = Bytes.toBytes("pdt")
  val paymentDebitCountCol = Bytes.toBytes("pdc")
  val paymentPaypalTotalCol = Bytes.toBytes("ppt")
  val paymentPaypalCountCol = Bytes.toBytes("ppc")


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

  def generateRowKey(accountId:String, appId:String,
                     eventTimestamp:Long, numOfSalts:Int): Array[Byte] = {
    val salt = StringUtils.leftPad(
      Math.abs(accountId.hashCode % numOfSalts).toString, 4, "0")

    Bytes.toBytes(salt + ":" +
      accountId + ":" +
      appId + ":" +
      StringUtils.leftPad(eventTimestamp.toString, 11, "0"))
  }




  def generateRowKey(accountMart: AccountMart, numOfSalts:Int): Array[Byte] = {
    generateRowKey(accountMart.accountId, accountMart.appId, numOfSalts)
  }

  def generateRowKey(accountId:String, appId:String, numOfSalts:Int): Array[Byte] = {
    val salt = StringUtils.leftPad(
      Math.abs(accountId.hashCode % numOfSalts).toString, 4, "0")

    Bytes.toBytes(salt + ":" +
      accountId + ":" +
      appId)
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

  def generatePut(accountMart: AccountMart, numOfSalt:Int): Put = {

    val put = new Put(generateRowKey(accountMart, numOfSalt))
    put.addColumn(columnFamily, signOnCountCol, Bytes.toBytes(accountMart.signOnCount.toString))
    put.addColumn(columnFamily, winCountCol, Bytes.toBytes(accountMart.winCount.toString))
    put.addColumn(columnFamily, loseCountCol, Bytes.toBytes(accountMart.loseCount.toString))
    put.addColumn(columnFamily, purchaseTotalCol, Bytes.toBytes(accountMart.purchaseTotal.toString))
    put.addColumn(columnFamily, paymentCreditTotalCol, Bytes.toBytes(accountMart.paymentCreditTotal.toString))
    put.addColumn(columnFamily, paymentCreditCountCol, Bytes.toBytes(accountMart.paymentCreditCount.toString))
    put.addColumn(columnFamily, paymentDebitTotalCol, Bytes.toBytes(accountMart.paymentDebitTotal.toString))
    put.addColumn(columnFamily, paymentDebitCountCol, Bytes.toBytes(accountMart.paymentDebitCount.toString))
    put.addColumn(columnFamily, paymentPaypalTotalCol, Bytes.toBytes(accountMart.paymentPaypalTotal.toString))
    put.addColumn(columnFamily, paymentPaypalCountCol, Bytes.toBytes(accountMart.paymentPaypalCount.toString))

    put
  }

  def convertToAccountMart(result:Result): AccountMart = {
    val keyParts = Bytes.toString(result.getRow).split(":")
    val accountId = keyParts(1)
    val appId = keyParts(2)

    val signOnCount = getResultString(result, signOnCountCol).toLong
    val winCount = getResultString(result, winCountCol).toLong
    val loseCount = getResultString(result, loseCountCol).toLong
    val purchaseTotal = getResultString(result, purchaseTotalCol).toDouble
    val paymentCreditTotal = getResultString(result, paymentCreditTotalCol).toDouble
    val paymentCreditCount = getResultString(result, paymentCreditCountCol).toLong
    val paymentDebitTotal = getResultString(result, paymentDebitTotalCol).toDouble
    val paymentDebitCount = getResultString(result, paymentDebitCountCol).toLong
    val paymentPaypalTotal = getResultString(result, paymentPaypalTotalCol).toDouble
    val paymentPaypalCount = getResultString(result, paymentPaypalCountCol).toLong

    new AccountMart(accountId,
      appId,
      signOnCount,
      winCount,
      loseCount,
      purchaseTotal,
      paymentCreditTotal,
      paymentCreditCount,
      paymentDebitTotal,
      paymentDebitCount,
      paymentPaypalTotal,
      paymentPaypalCount)

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
