package com.cloudera.sa.apptrans.model

import javax.xml.bind.annotation.{XmlAccessType, XmlAccessorType, XmlRootElement}

import org.apache.spark.sql.Row

@XmlRootElement(name = "AppEvent")
@XmlAccessorType(XmlAccessType.FIELD)
class AppEvent(val accountId: String,
               val appId: String,
               val eventTimestamp: Long,
               val eventId: String,
               val eventType:String,
               val purchase: Double,
               val paymentType: String,
               val sessionId: String,
               val latitude: Double,
               val longitude: Double) {

  override def toString():String = {
    accountId + "," +
      appId + ","+
      eventTimestamp + "," +
      eventId + "," +
      eventType + "," +
      purchase + "," +
      paymentType + "," +
      sessionId + "," +
      latitude + "," +
      longitude
  }

  def toRow():Row = {
    Row(accountId,
      appId,
      eventTimestamp,
      eventId,
      eventType,
      purchase,
      paymentType,
      sessionId,
      latitude,
      longitude)
  }
}

object AppEventBuilder {
  def build(csv:String): AppEvent = {
    val cells = csv.split(",")
    new AppEvent(
      cells(0),
      cells(1),
      cells(2).toLong,
      cells(3),
      cells(4),
      cells(5).toDouble,
      cells(6),
      cells(7),
      cells(8).toDouble,
      cells(9).toDouble)
  }

  def build(row:Row): AppEvent = {
    new AppEvent(
      row.getString(row.fieldIndex("account_id")),
      row.getString(row.fieldIndex("app_id")),
      row.getLong(row.fieldIndex("event_timestamp")),
      row.getString(row.fieldIndex("event_id")),
      row.getString(row.fieldIndex("event_type")),
      row.getDouble(row.fieldIndex("purchase")),
      row.getString(row.fieldIndex("payment_type")),
      row.getString(row.fieldIndex("sessionId")),
      row.getDouble(row.fieldIndex("latitude")),
      row.getDouble(row.fieldIndex("longitude")))
  }
}

object AppEventConst {
  val EVENT_TYPE_BUY = "buy"
  val EVENT_TYPE_LOGIN = "login"
  val EVENT_TYPE_LOGOUT = "logout"
  val EVENT_TYPE_WIN = "win"
  val EVENT_TYPE_LOSE = "lose"
  val PAYMENT_TYPE_CREDIT = "credit"
  val PAYMENT_TYPE_DEBIT = "debit"
  val PAYMENT_TYPE_PAYPAL = "paypal"
}
