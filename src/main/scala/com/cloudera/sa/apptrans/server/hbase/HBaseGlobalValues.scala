package com.cloudera.sa.apptrans.server.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HBaseGlobalValues {
  var appEventTableName = "app-event"
  var accountMartTableName = "account-mart"
  var numberOfSalts = 10000
  var connection:Connection = null

  def init(conf:Configuration, numberOfSalts:Int, customerTableName:String): Unit = {
    connection = ConnectionFactory.createConnection(conf)
    this.numberOfSalts = numberOfSalts
    this.accountTableName = customerTableName
  }
}
