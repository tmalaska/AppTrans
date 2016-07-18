package com.cloudera.sa.apptrans.generator

import java.io.File
import java.util.{Calendar, Random, UUID}

import com.cloudera.sa.apptrans.common.KafkaProducerUntil
import com.cloudera.sa.apptrans.model.{AppEvent, AppEventConst}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.mutable
import scala.io.Source

object AppEventProducer {
  var r = new Random()
  val latLongCacheList = new mutable.MutableList[(Double, Double)]
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<brokerList> <topicName> <sleepPerRecord> <acks> <linger.ms> <producer.type> <batch.size>" +
        " <NumOfAccounts> <NumOfApps> <NumOfRecords> <latLongCsvFile> <NumOfLayLongCache>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicName = args(1)
    val sleepPerRecord = args(2).toInt
    val acks = args(3).toInt
    val lingerMs = args(4).toInt
    val producerType = args(5) //"async"
    val batchSize = args(6).toInt
    val numOfAccounts = args(7).toInt
    val numOfApps = args(7).toInt
    val numOfRecords = args(8).toInt
    val latLongFileLoc = args(9)
    val latLongCacheSize = args(10).toInt

    val it = Source.fromFile(new File(latLongFileLoc)).getLines()
    for (i <- 0 until latLongCacheSize) {
      if (it.hasNext) {
        val cells = it.next().split(',')
        latLongCacheList.+=((cells(0).toDouble, cells(1).toDouble))
      }
    }


    runGenerator(kafkaBrokerList, kafkaTopicName, sleepPerRecord, acks, lingerMs, producerType, batchSize, numOfAccounts, numOfApps, numOfRecords)
  }
  def runGenerator(kafkaBrokerList:String,
                   kafkaTopicName:String,
                   sleepPerRecord:Int,
                   acks:Int,
                   lingerMs:Int,
                   producerType:String,
                   batchSize:Int,
                   numOfAccounts:Int,
                   numOfApps:Int,
                   numOfRecords:Int): Unit = {


    val customers = makeAccountss(numOfAccounts, numOfApps)

    val kafkaProducer = KafkaProducerUntil.getNewProducer(kafkaBrokerList, acks, lingerMs, producerType, batchSize)

    val startTime = System.currentTimeMillis()

    for (i <- 0 until numOfRecords) {
      val customer = customers.get(r.nextInt(customers.size)).get

      val currentTime:Calendar = Calendar.getInstance()
      currentTime.setTimeInMillis(startTime + i * 60000 * 2)

      val message = new ProducerRecord[String, String](kafkaTopicName,
        customer.accountId, customer.generateEvent(currentTime).toString())

      kafkaProducer.send(message)

      if (i % 100 == 0) {
        print(".")
        Thread.sleep(sleepPerRecord)
      }

      if (i % 10000 == 0) {
        println(":" + i + " -> sleeping:" + sleepPerRecord)
      }

    }
  }

  def makeAccountss(numOfAccount:Int, numOfApps:Int): mutable.MutableList[Account] = {
    val result = new mutable.MutableList[Account]

    for (i <- 0 until numOfAccount) {

      val paymentWeightR = r.nextInt(100)
      val eventWeightR = r.nextInt(100)


      val paymentTypeWeight:List[(Int, String)] = if (paymentWeightR < 40) {
        Seq((90, AppEventConst.PAYMENT_TYPE_CREDIT),
          (5,AppEventConst.PAYMENT_TYPE_DEBIT),
          (5,AppEventConst.PAYMENT_TYPE_PAYPAL)).toList
      } else if (paymentWeightR < 80) {
        Seq((10, AppEventConst.PAYMENT_TYPE_CREDIT),
          (15,AppEventConst.PAYMENT_TYPE_DEBIT),
          (75,AppEventConst.PAYMENT_TYPE_PAYPAL)).toList
      } else {
        Seq((30, AppEventConst.PAYMENT_TYPE_CREDIT),
          (37,AppEventConst.PAYMENT_TYPE_DEBIT),
          (33,AppEventConst.PAYMENT_TYPE_PAYPAL)).toList
      }

      val eventTypeWeight:List[(Int, String)] = if (eventWeightR < 40) {
        Seq((10, AppEventConst.EVENT_TYPE_BUY),
          (40, AppEventConst.EVENT_TYPE_LOSE),
          (40, AppEventConst.EVENT_TYPE_WIN),
          (10, AppEventConst.EVENT_TYPE_LOGIN)).toList
      } else if (eventWeightR < 80) {
        Seq((1, AppEventConst.EVENT_TYPE_BUY),
          (20, AppEventConst.EVENT_TYPE_LOSE),
          (60, AppEventConst.EVENT_TYPE_WIN),
          (19, AppEventConst.EVENT_TYPE_LOGIN)).toList
      } else {
        Seq((20, AppEventConst.EVENT_TYPE_BUY),
          (30, AppEventConst.EVENT_TYPE_LOSE),
          (30, AppEventConst.EVENT_TYPE_WIN),
          (20, AppEventConst.EVENT_TYPE_LOGIN)).toList
      }


      var appWeights = new mutable.MutableList[(Int, String)]
      val numOfAppsPerAccount = r.nextInt(7) + 5
      for (i <- 0 until numOfAppsPerAccount) {
        appWeights.+=((r.nextInt(25), r.nextInt(numOfApps).toString))
      }


      val account = new Account(i.toString,
        r.nextInt(100),
        r.nextInt(100),
        r.nextInt(100),
        r.nextInt(100),
        paymentTypeWeight,
        appWeights.toList,
        eventTypeWeight
      )
      result += (account)
    }
    result
  }

  class Account(val accountId:String,
                 val weekDaySpendCount:Int,
                 val weekDaySpendAmount:Int,
                 val weekEndSpendCount:Int,
                 val weekEndSpendAmount:Int,
                 val paymentTypeWeights:List[(Int, String)],
                 val appWeights:List[(Int, String)],
                 val eventTypeWeights:List[(Int, String)]) {
    var currentDayOfWeek = 0
    var transactions = 0
    var appWeightTotal = 0
    var currentSession = UUID.randomUUID().toString

    for (elem <- appWeights) {
      appWeightTotal += elem._1
    }


    def generateEvent(timeStamp:Calendar): AppEvent = {
      transactions += 1
      if (currentDayOfWeek > 0 && currentDayOfWeek < 6) {
        subGenerateTransaction(timeStamp, weekDaySpendAmount)
      } else {
        subGenerateTransaction(timeStamp, weekEndSpendAmount)
      }
    }

    private def subGenerateTransaction(timeStamp:Calendar,
                                       amount:Int): AppEvent = {

      var paymentRandom = r.nextInt(99) + 1
      var paymentIndex = -1
      while (paymentRandom > 0) {
        paymentIndex += 1
        paymentRandom -= paymentTypeWeights(paymentIndex)._1
      }

      var eventRandom = r.nextInt(99) + 1
      var eventIndex = -1
      while (eventRandom > 0) {
        eventIndex += 1
        eventRandom -= eventTypeWeights(eventIndex)._1
      }

      var appRandom = r.nextInt(appWeightTotal-1)+ 1
      var appIndex = -1
      while (appRandom > 0) {
        appIndex += 1
        appRandom -= appWeights(appIndex)._1
      }

      val finalAmount:Double = if (eventTypeWeights(eventIndex)._2.
        equals(AppEventConst.EVENT_TYPE_BUY)) {
        0
      } else if (r.nextBoolean()) {
        amount * (1 + r.nextInt(30)/100.0)
      } else {
        amount * (1 - r.nextInt(30)/100.0)
      }

      if (eventTypeWeights(eventIndex)._2.equals(AppEventConst.EVENT_TYPE_LOGIN)) {
        currentSession = UUID.randomUUID().toString
      }

      val latLong = latLongCacheList(r.nextInt(latLongCacheList.size - 1))

      new AppEvent(accountId,
        appWeights(appIndex)._2,
        timeStamp.getTimeInMillis,
        UUID.randomUUID().toString,
        eventTypeWeights(eventIndex)._2,
        finalAmount,
        paymentTypeWeights(paymentIndex)._2,
        currentSession,
        latLong._1,
        latLong._2)
    }
  }


}
