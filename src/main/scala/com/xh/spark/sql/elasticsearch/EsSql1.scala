package com.xh.spark.sql.elasticsearch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

case class Person(name: String, addr: String, age: Int)

object EsSql1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.index.auto.create", "true")
      .set("es.nodes", "hadoop01,hadoop02,hadoop03")
      .set("es.port", "9200")

    val spark = SparkSession.builder()
      .appName("Demo")
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    // create DataFrame
    val dataSet = List(
      ("spark", "US", 8),
      ("flink", "Germany", 5),
      ("mxnet", "US", 3))
    val rdd = sc.makeRDD(dataSet).map(p => Person(p._1, p._2, p._3))
    val df = rdd.toDF()
    df.saveToEs("/people")
    spark.stop()
  }
}
