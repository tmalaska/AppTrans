package com.cloudera.sa.apptrans.model

import javax.xml.bind.annotation.{XmlAccessType, XmlAccessorType, XmlRootElement}

import org.apache.spark.sql.Row

@XmlRootElement(name = "CustomerMart")
@XmlAccessorType(XmlAccessType.FIELD)
class AccountMart(val accountId:String,
                  val appId:String,
                  val signOnCount:Long,
                  val winCount:Long,
                  val loseCount:Long,
                  val purchaseTotal:Double,
                  val paymentCreditTotal:Double,
                  val paymentCreditCount:Long,
                  val paymentDebitTotal:Double,
                  val paymentDebitCount:Long,
                  val paymentPaypalTotal:Double,
                  val paymentPaypalCount:Long) {
  override def toString():String = {
    accountId + "," +
      appId + ","+
      signOnCount + "," +
      winCount + "," +
      loseCount + "," +
      purchaseTotal + "," +
      paymentCreditTotal + "," +
      paymentCreditCount + "," +
      paymentDebitTotal + "," +
      paymentDebitCount + "," +
      paymentPaypalTotal + "," +
      paymentPaypalCount
  }

  def + (am: AccountMart): AccountMart = {
    new AccountMart(accountId,
    appId,
    signOnCount + am.signOnCount,
    winCount + am.winCount,
    loseCount + am.loseCount,
    purchaseTotal + am.purchaseTotal,
    paymentCreditTotal + am.paymentCreditTotal,
    paymentCreditCount + am.paymentCreditCount,
    paymentDebitTotal + am.paymentDebitTotal,
    paymentDebitCount + am.paymentDebitCount,
    paymentPaypalTotal + am.paymentPaypalTotal,
    paymentPaypalCount + am.paymentPaypalCount)
  }
}

object AccountMartBuilder {
  def build(csv:String): AccountMart = {
    val cells = csv.split(",")
    new AccountMart(
      cells(0),
      cells(1),
      cells(2).toLong,
      cells(3).toLong,
      cells(4).toLong,
      cells(5).toDouble,
      cells(6).toDouble,
      cells(7).toLong,
      cells(8).toDouble,
      cells(9).toLong,
      cells(10).toDouble,
      cells(11).toLong)
  }

  def build(row:Row): AccountMart = {
    new AccountMart(
      row.getString(row.fieldIndex("account_id")),
      row.getString(row.fieldIndex("app_id")),
      row.getLong(row.fieldIndex("sign_on_count")),
      row.getLong(row.fieldIndex("win_count")),
      row.getLong(row.fieldIndex("lose_count")),
      row.getDouble(row.fieldIndex("purchase_total")),
      row.getDouble(row.fieldIndex("payment_credit_total")),
      row.getLong(row.fieldIndex("payment_credit_count")),
      row.getDouble(row.fieldIndex("payment_debit_total")),
      row.getLong(row.fieldIndex("payment_debit_count")),
      row.getDouble(row.fieldIndex("payment_paypal_total")),
      row.getLong(row.fieldIndex("payment_paypal_count")))
  }
}
