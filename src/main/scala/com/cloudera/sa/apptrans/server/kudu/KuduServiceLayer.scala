package com.cloudera.sa.apptrans.server.kudu

import javax.ws.rs.core.MediaType
import javax.ws.rs.{QueryParam, _}

import com.cloudera.sa.apptrans.model.{AccountMart, AppEvent}
import org.kududb.client.KuduPredicate

import scala.collection.mutable

@Path("rest")
class KuduServiceLayer {

  @GET
  @Path("hello")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def hello(): String = {
    "Hello World"
  }

  @GET
  @Path("customer/{customerId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCustomerMart (@PathParam("customerId") customerId:String): AccountMart = {
    val kuduClient = KuduGlobalValues.kuduClient
    val custTable = KuduGlobalValues.kuduClient.openTable(KuduGlobalValues.accountMartTableName)

    val schema = custTable.getSchema
    val customerIdCol = schema.getColumn(customerId)

    val scanner = kuduClient.newScannerBuilder(custTable).
      addPredicate(KuduPredicate.
        newComparisonPredicate(customerIdCol, KuduPredicate.ComparisonOp.EQUAL, customerId)).
      build()

    var accountMart:AccountMart = null

    while (scanner.hasMoreRows) {
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        val rowResult = rows.next()

        accountMart = new AccountMart(
          rowResult.getString("account_id"),
          rowResult.getString("app_id"),
          rowResult.getLong("sign_on_count"),
          rowResult.getLong("win_count"),
          rowResult.getLong("lose_count"),
          rowResult.getDouble("purchase_total"),
          rowResult.getDouble("payment_credit_total"),
          rowResult.getLong("payment_credit_count"),
          rowResult.getDouble("payment_debit_total"),
          rowResult.getLong("payment_debit_count"),
          rowResult.getDouble("payment_paypal_total"),
          rowResult.getLong("payment_paypal_count"))
      }
    }

    accountMart
  }

  @GET
  @Path("customer/ts/{customerId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCustomerLineage (@PathParam("customerId") customerId:String,
                          @QueryParam("startTime") startTime:Long = Long.MaxValue,
                          @QueryParam("endTime")  endTime:Long = Long.MinValue): List[AppEvent] = {
    val kuduClient = KuduGlobalValues.kuduClient
    val custTable = KuduGlobalValues.kuduClient.openTable(KuduGlobalValues.appEventTableName)

    val schema = custTable.getSchema
    val customerIdCol = schema.getColumn(customerId)
    val eventTimeStampCol = schema.getColumn("event_timestamp")

    val scanner = kuduClient.newScannerBuilder(custTable).
      addPredicate(KuduPredicate.
      newComparisonPredicate(customerIdCol, KuduPredicate.ComparisonOp.EQUAL, customerId)).
      addPredicate(KuduPredicate.
      newComparisonPredicate(eventTimeStampCol, KuduPredicate.ComparisonOp.GREATER, startTime)).
      addPredicate(KuduPredicate.
      newComparisonPredicate(eventTimeStampCol, KuduPredicate.ComparisonOp.LESS, endTime)).
      batchSizeBytes(1000000).build()


    val appEventList = new mutable.MutableList[AppEvent]

    while (scanner.hasMoreRows) {
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        val rowResult = rows.next()

        val appEvent = new AppEvent(
          rowResult.getString("account_id"),
          rowResult.getString("app_id"),
          rowResult.getLong("event_timestamp"),
          rowResult.getString("event_id"),
          rowResult.getString("event_type"),
          rowResult.getDouble("purchase"),
          rowResult.getString("payment_type"),
          rowResult.getString("session_id"),
          rowResult.getDouble("latitude"),
          rowResult.getDouble("longitude"))

        appEventList += appEvent
      }
    }

    appEventList.toList
  }
}
