package com.oskarro

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object StreamSparkConsumer {

  case class BusModel(Lines: String, Lon: String, VehicleNumber: String, Time: String, Lat: String, Brigade: String)

  def main(args: Array[String]): Unit = {
    readStreamCurrentLocationOfVehicles(
      "temat_bus01",
      "localhost:9092",
      "transport",
      "bus_spark2")
  }

  def readStreamCurrentLocationOfVehicles(topic: String,
                                          kafkaServers: String,
                                          cassandraKeyspace: String,
                                          cassandraTable: String): Unit = {

    import org.apache.spark.SparkContext
    import org.apache.spark.streaming._

    // Spark Session create
    val sparkSession = SparkSession
      .builder()
      .appName("MyApp")
      .config("spark.casandra.connection.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("ERROR")

    val batchInterval = 10
    val ssc = new StreamingContext(sparkContext, Seconds(batchInterval))

    import org.apache.spark.streaming.kafka010._

    val r = scala.util.Random

    // Generate a new Kafka Consumer group id every run
    val groupId = s"stream-checker-v${r.nextInt.toString}"

    import org.apache.kafka.common.serialization.StringDeserializer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    object FinishedBatchesCounter {
      @volatile private var instance: LongAccumulator = null

      def getInstance(sc: SparkContext): LongAccumulator = {
        if (instance == null) {
          synchronized {
            if (instance == null) {
              instance = sc.longAccumulator("FinishedBatchesCounter")
            }
          }
        }
        instance
      }
    }

    import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
    import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](List(topic), kafkaParams)
    )


    import scala.util.parsing.json.JSON
    val messages = stream
      .map(rec => rec.value())
      .flatMap(rec => {
        JSON.parseFull(rec).map(rawMap => {
          val map = rawMap.asInstanceOf[Map[String, Object]]
          BusModel(
            map("Lines").toString,
            map("Lon").toString,
            map("VehicleNumber").toString,
            map("Time").toString,
            map("Lat").toString,
            map("Brigade").toString
          )
        })
      })

    // Cache DStream now, it'll speed up most of the operations below
    messages.cache()


    // How many batches to run before terminating
    val batchesToRun = 10

    // Counting batches and terminating after 'batchesToRun'
    messages.foreachRDD { rdd =>

      val dinishedBatchesCounter = FinishedBatchesCounter.getInstance(sparkContext)

      println(s"--- Batch ${dinishedBatchesCounter.count + 1} ---")
      println("Processed messages in this batch: " + rdd.count())

      if (dinishedBatchesCounter.count >= batchesToRun - 1) {
        ssc.stop()
      } else {
        dinishedBatchesCounter.add(1)
      }
    }

    // Printing aggregation for the platforms:
    messages
      .map(msg => (msg.VehicleNumber, 1))
      .reduceByKey(_ + _)
      .print()

    // Printing messages with 'weird' uids
    val weirdUidMessages = messages
      .filter(msg =>
        msg.VehicleNumber == "NULL"
          || msg.VehicleNumber == ""
          || msg.VehicleNumber == " "
          || msg.VehicleNumber.length < 10
      )

    weirdUidMessages.print(20)
    weirdUidMessages.count().print()
    ssc.start
    ssc.awaitTermination()
  }
}
