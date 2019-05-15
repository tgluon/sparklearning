package com.xh.spark.sql.practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ShuffleRemoteDebug {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("ShuffleRemoteDebug")
      .set("spark.master", "local[*]")
    //      .set("spark.submit.deployMode", "client")
    //      .set("yarn.resourcemanager.hostname", "hadoop02")
    //      .set("spark.executor.instances", "2")
    //      .set("spark.dynamicAllocation.enabled", "false")
    //      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    //      .set("spark.sql.join.preferSortMergeJoin", "true")
    //      .setJars(List("/home/hadoop/worker/sparklearning/target/sparklearning-1.0-SNAPSHOT.jar",
    //        "/home/hadoop/worker/sparklearning/target/sparklearning-1.0-SNAPSHOT-jar-with-dependencies.jar"
    //      ))

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    val df = List(
      ("站点1", "2018-01-01", 500),
      ("站点1", "2018-01-02", 450),
      ("站点1", "2018-01-03", 100),
      ("站点1", "2018-01-04", 200),
      ("站点1", "2018-01-05", 600),
      ("站点1", "2018-01-06", 800),
      ("站点1", "2018-01-07", 900),
      ("站点1", "2018-01-08", 450),
      ("站点1", "2018-01-09", 600),
      ("站点1", "2018-01-10", 900),
      ("站点2", "2018-01-01", 200),
      ("站点2", "2018-01-02", 400),
      ("站点2", "2018-01-03", 600),
      ("站点2", "2018-01-04", 300),
      ("站点2", "2018-01-05", 500),
      ("站点2", "2018-01-06", 900),
      ("站点2", "2018-01-07", 700),
      ("站点2", "2018-01-08", 600),
      ("站点2", "2018-01-09", 800),
      ("站点2", "2018-01-10", 300),
      ("站点2", "2018-01-02", 900),
      ("站点3", "2018-01-01", 90),
      ("站点3", "2018-01-02", 20),
      ("站点3", "2018-01-03", 50),
      ("站点3", "2018-01-04", 60),
      ("站点3", "2018-01-05", 30),
    ).toDF("site", "date", "user_cnt")

    //    val siteRDD = spark.sparkContext.parallelize(df)
    //    siteRDD.map(t => (t._1, t._3)).reduceByKey(_ + _).foreach(t=>println(t._1,t._2))
    //    df.groupBy("site").sum("user_cnt").show()


    df.createOrReplaceTempView("site_info")
    spark.sql(
      """
        |select site, sum(user_cnt) from site_info group by site
      """.stripMargin)
      .show(10000)

    spark.stop()
  }
}
