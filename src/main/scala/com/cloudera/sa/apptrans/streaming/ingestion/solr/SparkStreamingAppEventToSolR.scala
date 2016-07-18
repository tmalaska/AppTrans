package com.cloudera.sa.apptrans.streaming.ingestion.solr

import java.text.SimpleDateFormat
import java.util.Date

import com.cloudera.sa.apptrans.common.SolrSupport
import com.cloudera.sa.apptrans.model.{AppEvent, AppEventBuilder}
import kafka.serializer.StringDecoder
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.cloud.ZooKeeperException
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingAppEventToSolR {
  def main(args: Array[String]): Unit = {
    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties().getProperty("java.home"))

    val v: ZooKeeperException = null

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<kafkaTopicList> " +
        "<checkpointDir>" +
        "<numberOfSeconds>" +
        "<runLocal>" +
        "<solrCollection>" +
        "<zkHost>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicList = args(1)
    val checkPointFolder = args(2)
    val numberOfSeconds = args(3).toInt
    val runLocal = args(4).equals("l")
    val solrCollection = args(5)
    val zkHost = args(6)

    println("kafkaBrokerList:" + kafkaBrokerList)
    println("kafkaTopicList:" + kafkaTopicList)
    println("numberOfSeconds:" + numberOfSeconds)
    println("runLocal:" + runLocal)
    println("solrCollection:" + solrCollection)
    println("zkHost:" + zkHost)

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("Spark Streaming Ingestion to SolR")
      new SparkContext(sparkConf)
    }
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))

    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val appEventDStream = messageStream.map { case (key, value) => AppEventBuilder.build(value) }

    val solrDocumentDStream = appEventDStream.map(convertToSolRDocuments)

    SolrSupport.indexDStreamOfDocs(zkHost,
      solrCollection,
      100,
      solrDocumentDStream)

    ssc.checkpoint(checkPointFolder)
    ssc.start()
    ssc.awaitTermination()
  }

  def convertToSolRDocuments(appEvent:AppEvent): SolrInputDocument = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    val doc: SolrInputDocument = new SolrInputDocument
    doc.addField("id", appEvent.eventType + ":" + appEvent.eventId)
    doc.addField("account_id", appEvent.accountId)
    doc.addField("app_id", appEvent.appId)
    doc.addField("event_time_stamp", dateFormat.format(new Date(appEvent.eventTimestamp)))
    doc.addField("event_id", appEvent.eventId)
    doc.addField("event_type", appEvent.eventType)
    doc.addField("purchase", appEvent.purchase)
    doc.addField("payment_type", appEvent.paymentType)
    doc.addField("session_id", appEvent.sessionId)
    doc.addField("latitude", appEvent.latitude)
    doc.addField("longitude", appEvent.longitude)

    doc
  }
}
