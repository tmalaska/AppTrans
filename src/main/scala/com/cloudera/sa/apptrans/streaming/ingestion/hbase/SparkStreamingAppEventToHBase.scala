package com.cloudera.sa.apptrans.streaming.ingestion.hbase

import java.io.File

import com.cloudera.sa.apptrans.model.{AccountMart, AppEventBuilder}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.solr.common.cloud.ZooKeeperException
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingAppEventToHBase {
  def main(args: Array[String]): Unit = {
    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties().getProperty("java.home"))

    val v:ZooKeeperException = null

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<kafkaTopicList> " +
        "<numberOfSeconds>" +
        "<runLocal>" +
        "<hbaseTable>" +
        "<numOfSalts>" +
        "<checkpointDir>" +
        "<hbaseConfigFolder>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicList = args(1)
    val numberOfSeconds = args(2).toInt
    val runLocal = args(3).equals("l")
    val tableName = args(4)
    val numOfSalts = args(5).toInt
    val checkpointFolder = args(6)
    val hbaseConfigFolder = args(7)

    println("kafkaBrokerList:" + kafkaBrokerList)
    println("kafkaTopicList:" + kafkaTopicList)
    println("numberOfSeconds:" + numberOfSeconds)
    println("runLocal:" + runLocal)
    println("tableName:" + tableName)
    println("numOfSalts:" + numOfSalts)

    val sc:SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("Spark Streaming Ingestion to HBase")
      new SparkContext(sparkConf)
    }
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))

    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val conf = HBaseConfiguration.create()

    conf.addResource(new File(hbaseConfigFolder + "hbase-site.xml").toURI.toURL)

    val hbaseContext = new HBaseContext(sc, conf)

    val appEventDStream = messageStream.map{case(key, value) => AppEventBuilder.build(value)}

    appEventDStream.hbaseBulkPut(hbaseContext, TableName.valueOf(tableName), customer => {
      AppEventHBaseHelper.generatePut(customer, numOfSalts)
    })

    val mapDStream = appEventDStream.map(appEvent =>
      (appEvent.accountId + "," + appEvent.appId, appEvent.toAccountMart()))

    val aggDStream = mapDStream.updateStateByKey[AccountMart]((a:Seq[AccountMart], b:Option[AccountMart]) => {
      val aSum:AccountMart = a.reduce((a1, a2) => a1 + a2)
      val optional = if (b.isEmpty) {
        Option(aSum)
      } else {
        Option(aSum + b.get)
      }
      optional
    }).map(kv => {kv._2})


    aggDStream.hbaseBulkPut(hbaseContext, TableName.valueOf(tableName), accountMart => {
      AppEventHBaseHelper.generatePut(accountMart, numOfSalts)
    })

    ssc.checkpoint(checkpointFolder)
    ssc.start()
    ssc.awaitTermination()
  }
}
