package com.cloudera.sa.apptrans.streaming.ingestion.kudu

import com.cloudera.sa.apptrans.model.{AppEvent, AppEventBuilder}
import kafka.serializer.StringDecoder
import org.apache.solr.common.cloud.ZooKeeperException
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.kududb.client.{KuduClient, Operation}
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.spark.kudu.KuduContext

object SparkStreamingAppEventToKudu {
  def main(args: Array[String]): Unit = {
    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties().getProperty("java.home"))

    val v: ZooKeeperException = null

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<kafkaTopicList> " +
        "<numberOfSeconds>" +
        "<runLocal>" +
        "<kuduMaster>" +
        "<kuduAccountMartTable>",
        "<kuduAppEventTable",
        "<checkPointFolder>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicList = args(1)
    val numberOfSeconds = args(2).toInt
    val runLocal = args(3).equals("l")
    val kuduMaster = args(4)
    val kuduAccountMartTable = args(5)
    val kuduAppEventTable = args(6)
    val checkPointFolder = args(7)

    println("kafkaBrokerList:" + kafkaBrokerList)
    println("kafkaTopicList:" + kafkaTopicList)
    println("numberOfSeconds:" + numberOfSeconds)
    println("runLocal:" + runLocal)
    println("kuduMaster:" + kuduMaster)
    println("kuduAccountMartTable:" + kuduAccountMartTable)
    println("kuduAppEventTable:" + kuduAppEventTable)
    println("checkPointFolder:" + checkPointFolder)

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("Spark Streaming Ingestion to Kudu")
      new SparkContext(sparkConf)
    }
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))

    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val kuduContext = new KuduContext(kuduMaster)

    val appEventDStream = messageStream.map { case (key, value) =>
      AppEventBuilder.build(value)
    }

    appEventDStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        sendEntityToKudu(kuduAppEventTable, it, kuduContext.syncClient)
      })
    })

    println("--Starting Spark Streaming")
    ssc.checkpoint(checkPointFolder)
    ssc.start()
    ssc.awaitTermination()
  }

  def sendEntityToKudu(kuduAppEventTable: String, it: Iterator[AppEvent], kuduClient: KuduClient): Unit = {
    val table = kuduClient.openTable(kuduAppEventTable)
    val session = kuduClient.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

    it.foreach(appEvent => {

      val operation: Operation = table.newUpsert()

      if (operation != null) {
        val row = operation.getRow()

        row.addString("account_id", appEvent.accountId)
        row.addString("app_id", appEvent.appId)
        row.addLong("event_timestamp", appEvent.eventTimestamp)
        row.addString("event_id", appEvent.eventId)
        row.addString("event_type", appEvent.eventType)
        row.addDouble("purchase", appEvent.purchase)
        row.addString("payment_type", appEvent.paymentType)
        row.addString("session_id", appEvent.sessionId)
        row.addDouble("latitude", appEvent.latitude)
        row.addDouble("longitude", appEvent.longitude)

        session.apply(operation)
      }

    })
    session.flush()
    session.close()
  }
}
