package com.xh.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("WindowFunctionTest")
      .set("spark.master", "local[*]")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.read.parquet("src/main/resources/data/data.parquet")
    df.createTempView("basc")
    spark.sql(
      """
        |SELECT info_monitor_id,
        |       info_camera_id,
        |	   action_monitor_id,
        |	   action_camera_id,
        |    rank
        |from (
        |      SELECT info_monitor_id,
        |	         info_camera_id,
        |			 action_monitor_id,
        |			 action_camera_id,
        |			 row_number() OVER (PARTITION BY info_monitor_id  order by info_camera_id) rank
        |	 from basc
        |	 )a
        |	 where a.rank <3
      """.stripMargin).show()

    spark.stop()
  }
}
