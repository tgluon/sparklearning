package com.xh.spark.sql.practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 使用上一条数据填充下一条数据空值
  */
object NulllValuePadding {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("WindowFunctionTest")
      .set("spark.master", "yarn")
      .set("spark.local.ip", "192.168.8.84")
      .set("spark.driver.host", "hadoop04")
      .set("spark.submit.deployMode", "client")
      .set("yarn.resourcemanager.hostname", "hadoop02") // resourcemanager主机名
      .set("spark.executor.instances", "2")
      .setJars(List("/home/deeplearning/jar/sparklearning-1.0-SNAPSHOT.jar",
        "/home/deeplearning/jar/sparklearning-1.0-SNAPSHOT-jar-with-dependencies.jar"
      ))


    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    // 1. 读取数据
    val nullValuePaddingDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", true)
      .option("header", false)
      .load("src/main/resources/nullValuePadding.csv")
      .toDF("uid", "time", "event")
    nullValuePaddingDF.createOrReplaceTempView("test")
    spark.sql(
      """
        |select
        |      t1.uid,
        |      t1.time,
        |      t2.event
        |from
        |(
        |    select
        |          uid,
        |          time,
        |          event,
        |          row,
        |          all_row
        |      from
        |      (
        |      select
        |      uid,
        |      time,
        |      event,
        |      row_number() over(partition by case when event is not null and trim(event)<>'' then 1 else 0 end order by time asc) as row,
        |      row_number()over( order by time asc) as all_row
        |      from test
        |      )t
        |      where event is  null
        |)t1
        |left join
        |(
        |    select
        |          uid,
        |          time,
        |          event,
        |          row,
        |          all_row
        |    from
        |     (
        |      select
        |      uid,
        |      time,
        |      event,
        |      row_number()over(partition by case when event is not null and trim(event)<>'' then 1 else 0 end order by time asc) as row,
        |      row_number()over( order by time asc) as all_row
        |      from test
        |      )t
        |     where event is not  null
        |)t2
        |on  t1.all_row-t1.row=t2.row
        |union all
        |select
        |uid,
        |time,
        |event
        |from test
        |where event is not  null
      """.stripMargin).show()
    spark.stop()
  }
}
