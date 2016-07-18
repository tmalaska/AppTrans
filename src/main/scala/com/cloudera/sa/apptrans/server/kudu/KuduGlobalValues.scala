package com.cloudera.sa.apptrans.server.kudu

import org.kududb.client.KuduClient

object KuduGlobalValues {

  var appEventTableName = "app_event_kudu"
  var accountMartTableName = "account_mart_kudu"

  var kuduClient:KuduClient = null

  def init(kuduMaster:String,
           appEventTableName:String,
           accountMartTableName:String): Unit = {
    kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    this.appEventTableName = appEventTableName
    this.accountMartTableName = accountMartTableName
  }

}
