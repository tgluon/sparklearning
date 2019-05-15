package com.xh.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirect {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("KafkaDirect")
      .setMaster("yarn-client")
      .set("yarn.resourcemanager.hostname", "hadoop02") // resourcemanager主机名
      .set("spark.executor.instances", "2")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅的关闭
      .set("spark.streaming.backpressure.enabled", "true") // 激活削峰功能
      .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
      .set("spark.streaming.concurrentJobs", "10") //提高Job并发数,默认为1
      .set(".spark.streaming.kafka.maxRatePerPartition", "2000") // 设置每秒每个分区最大获取日志数，控制处理数据量，保证数据均匀处理。
      .set("spark.streaming.kafka.maxRetries", "50") // 获取topic分区leaders及其最新offsets时，调大重试次数。
      .setJars(List("/home/deeplearning/bigdata.jar"))

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val context = spark.sparkContext
    val ssc = new StreamingContext(context, Seconds(1))
    // 开启checkpoint
    ssc.checkpoint("hdfs://ns1/streaming/checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092,hadoop04:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicSet = Array("topicA", "topicB").toSet
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams))

    // 处理逻辑
    // 启动流
    ssc.start()
    ssc.awaitTermination()
  }
}
