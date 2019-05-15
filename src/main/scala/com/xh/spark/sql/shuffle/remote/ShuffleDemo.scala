package com.xh.spark.sql.shuffle.remote

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ShuffleDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("WindowFunctionTest")
      .set("spark.master", "local[*]")
//      .set("spark.sql.warehouse.dir", "hdfs://ns1/hive/warehouse")
//      .set("spark.submit.deployMode", "client")
//      .set("yarn.resourcemanager.hostname", "hadoop02")
//      .set("spark.executor.instances", "2")
//      .set("spark.dynamicAllocation.enabled", "false")
//      .setJars(List("/home/hadoop/worker/sparklearning/target/sparklearning-1.0-SNAPSHOT.jar",
//        "/home/hadoop/worker/sparklearning/target/sparklearning-1.0-SNAPSHOT-jar-with-dependencies.jar"
//      ))

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

//    spark.sql(
//      """
//        |select temp_product.product_model,
//        |       temp_product.category_second,
//        |       temp_jd_vcp_goods_day.ds,
//        |       temp_jd_vcp_goods_day.sale_num,
//        |       temp_jd_vcp_goods_day.income
//        |from   ods.temp_product
//        |join   ods.temp_jd_vcp_goods_day
//        |  on   temp_product.goods_id = temp_jd_vcp_goods_day.goods_id
//      """.stripMargin).show()


    spark.stop()
  }
}
