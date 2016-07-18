package com.cloudera.sa.apptrans.streaming.ingestion.hdfs

import com.cloudera.sa.apptrans.model.AppEventBuilder
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.solr.common.cloud.ZooKeeperException
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingAppEventToHDFS {

  val tmpTooSmallFolder = "TooSmall"
  val tmpWritingFolder = "WriteFolder"

  def main(args:Array[String]): Unit = {
    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties().getProperty("java.home"))

    val v: ZooKeeperException = null

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<kafkaTopicList> " +
        "<checkpointDir>" +
        "<runLocal>" +
        "<tmpFolder>" +
        "<hiveTableFolder>" +
        "<hiveTableName>" +
        "<minFileSize>" +
        "<checkPointFolder>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicList = args(1)
    val numberOfSeconds = args(2).toInt
    val runLocal = args(3).equals("l")
    val tmpFolder = args(4)
    val hiveTableFolder = args(5)
    val hiveTableName = args(6)
    val minFileSize = args(7).toInt
    val checkPointFolder = args(8)

    println("kafkaBrokerList:" + kafkaBrokerList)
    println("kafkaTopicList:" + kafkaTopicList)
    println("numberOfSeconds:" + numberOfSeconds)
    println("runLocal:" + runLocal)
    println("tmpFolder:" + tmpFolder)
    println("hiveTableFolder:" + hiveTableFolder)
    println("hiveTableName:" + hiveTableName)
    println("minFileSize:" + minFileSize)
    println("checkPointFolder:" + checkPointFolder)

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("Spark Streaming Ingestion to HDFS")
      new SparkContext(sparkConf)
    }
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))
    val sqc = new SQLContext(sc)

    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val schema = sqc.sql("select * from " + hiveTableName + " limit 0").schema

    var offsetRanges = Array[OffsetRange]()
    var oldPartitionZeroFromOffSet:Long = -1
    var newPartitionZeroFromOffSet:Long = -1

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    messageStream.foreachRDD(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        if (o.partition == 0) {
          val superOldPartitionZeroFromOffSet = oldPartitionZeroFromOffSet
          oldPartitionZeroFromOffSet = newPartitionZeroFromOffSet
          newPartitionZeroFromOffSet = o.fromOffset

          val superOldTooSmallString = tmpFolder + "/" +
            tmpTooSmallFolder + "/" +
            superOldPartitionZeroFromOffSet

          Cached.getFS.delete(new Path(superOldTooSmallString), true)
        }
      }
    })

    val rowDStream = messageStream.map { case (key, value) =>
      AppEventBuilder.build(value).toRow()
    }

    rowDStream.foreachRDD(rdd => {
      val sqc = new SQLContext(sc)

      val tooSmallString = tmpFolder + "/" + tmpTooSmallFolder + "/" + oldPartitionZeroFromOffSet
      val tooSmallPath = new Path(tooSmallString)
      val writingFolderString = tmpFolder + "/" + tmpWritingFolder
      val writingfolderPath = new Path(writingFolderString)
      val tooSmallNewString = tmpFolder + "/" + tmpTooSmallFolder + "/" + newPartitionZeroFromOffSet
      val tooSmallNewPath = new Path(tooSmallString)

      val newDf = sqc.createDataFrame(rdd, Cached.getSchema(hiveTableName))

      val finalDf = if (oldPartitionZeroFromOffSet >= 0) {
        val statusList = Cached.getFS.listStatus(tooSmallPath)
        if (statusList.length > 0) {
          val smallDf = sqc.read.parquet(tooSmallString)
          smallDf.unionAll(newDf)
        } else {
          newDf
        }
      } else {
        newDf
      }
      Cached.getFS.delete(writingfolderPath, true)
      Cached.getFS.mkdirs(writingfolderPath)
      finalDf.write.parquet(writingFolderString)

      val justWroteStatusIt = Cached.getFS.listStatus(writingfolderPath).iterator

      Cached.getFS.mkdirs(tooSmallNewPath)

      while (justWroteStatusIt.hasNext) {
        val currentStatus = justWroteStatusIt.next()
        if (currentStatus.getLen < minFileSize) {
          Cached.getFS.rename(currentStatus.getPath, tooSmallNewPath)
        } else {
          val name = currentStatus.getPath.getName

          val newName = name.substring(0, name.lastIndexOf('.') - 1) + newPartitionZeroFromOffSet +
            name.substring(name.lastIndexOf('.'))

          val newFilePath = new Path(writingfolderPath + "/" + newName)
          if (Cached.getFS.exists(newFilePath)) {
            Cached.getFS.delete(newFilePath, false)
          }
          Cached.getFS.rename(currentStatus.getPath, newFilePath)
        }
      }
    })

    println("--Starting Spark Streaming")
    ssc.checkpoint(checkPointFolder)
    ssc.start()
    ssc.awaitTermination()
  }
}

object Cached {
  val getFS = FileSystem.get(new Configuration())
  private var sqlContext:SQLContext = null
  private var schema:StructType = null

  def getSqlContext(sc:SparkContext): SQLContext = {
    if (sqlContext == null) {
      sqlContext = new SQLContext(sc)
    }
    sqlContext
  }

  def getSchema(tableName:String): StructType = {
    schema = sqlContext.sql("select * from " + tableName + " limit 0").schema
    schema
  }
}
