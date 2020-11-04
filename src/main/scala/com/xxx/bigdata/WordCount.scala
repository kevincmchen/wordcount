package com.xxx.bigdata

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object WordCount {

  def main(args: Array[String]): Unit = {
    var brokers: String = args(0)
    var topics: String = args(1)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.stopSparkContextByDefault", "true")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_streaming_group_0",
      "security.protocol"->"SASL_PLAINTEXT",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams))
    ssc.checkpoint("/tmp/spark-checkpoint")

    // Get the lines, split them into words, count the words and print
    var offsetRanges = Array[OffsetRange]()
    val lines = messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //TODO: fault tolerance: save offset to zk/mysql/hbase
      rdd
    }.map(_.value())

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    wordCounts.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
