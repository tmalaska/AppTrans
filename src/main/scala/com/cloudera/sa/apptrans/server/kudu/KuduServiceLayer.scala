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
  @Path("accountMart/{accountId}/{appId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccountMart (@PathParam("accountId") accountId:String,
                       @PathParam("appId") appId:String): AccountMart = {
    val kuduClient = KuduGlobalValues.kuduClient
    val custTable = KuduGlobalValues.kuduClient.openTable(KuduGlobalValues.accountMartTableName)

    val schema = custTable.getSchema
    val accountIdCol = schema.getColumn("account_id")
    val appIdCol = schema.getColumn("app_id")

    val scanner = kuduClient.newScannerBuilder(custTable).
      addPredicate(KuduPredicate.
        newComparisonPredicate(accountIdCol, KuduPredicate.ComparisonOp.EQUAL, accountId)).
      addPredicate(KuduPredicate.
        newComparisonPredicate(appIdCol, KuduPredicate.ComparisonOp.EQUAL, appId)).
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
  @Path("accountMart/{accountId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccountMartList (@PathParam("accountId") accountId:String): Array[AccountMart] = {
    val kuduClient = KuduGlobalValues.kuduClient
    val custTable = KuduGlobalValues.kuduClient.openTable(KuduGlobalValues.accountMartTableName)

    val schema = custTable.getSchema
    val accountIdCol = schema.getColumn(accountId)

    val scanner = kuduClient.newScannerBuilder(custTable).
      addPredicate(KuduPredicate.
        newComparisonPredicate(accountIdCol, KuduPredicate.ComparisonOp.EQUAL, accountId)).
      build()

    val accountMartList = new mutable.MutableList[AccountMart]

    while (scanner.hasMoreRows) {
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        val rowResult = rows.next()

        accountMartList += new AccountMart(
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

    accountMartList.toArray
  }

  @GET
  @Path("appEvent/ts/{accountId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAppEventLineage (@PathParam("accountId") accountId:String,
                          @QueryParam("startTime") startTime:Long = Long.MaxValue,
                          @QueryParam("endTime")  endTime:Long = Long.MinValue): List[AppEvent] = {
    val kuduClient = KuduGlobalValues.kuduClient
    val custTable = KuduGlobalValues.kuduClient.openTable(KuduGlobalValues.appEventTableName)

    val schema = custTable.getSchema
    val accountIdCol = schema.getColumn("account_id")
    val eventTimeStampCol = schema.getColumn("event_timestamp")

    val scanner = kuduClient.newScannerBuilder(custTable).
      addPredicate(KuduPredicate.
      newComparisonPredicate(accountIdCol, KuduPredicate.ComparisonOp.EQUAL, accountId)).
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
