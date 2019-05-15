package com.xh.spark.sql.practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 统计月份累计消费金额
  */

object AccumulatorAccess {

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

    val usersDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "false")
      .load("src/main/resources/access.csv")
      .toDF("name", "mounth", "amount")
    usersDF.createOrReplaceTempView("access")
    // 1.先求出每个用户当月访问次数
    val perMounthAmountDF = spark.sql(
      """
        |select name,
        |       mounth,
        |	      sum(amount) as amount
        |from access
        |group by name,mounth
      """.stripMargin)

    perMounthAmountDF.createOrReplaceTempView("tmp_per_mounth_access")

    // 2.自连接
    val joinPerMounthAccessDF = spark.sql(
      """
        |select a.name as aname,
        |       a.mounth as amounth,
        |	      a.amount as aamount,
        |	      b.name as bname,
        |	      b.mounth as bmounth,
        |	      b.amount as bamount
        |from   tmp_per_mounth_access a
        |join   tmp_per_mounth_access b
        |on     a.name = b.name
      """.stripMargin)
    joinPerMounthAccessDF.createOrReplaceTempView("temp_access_view")
    // 3.每个用户每个月消费金额及月份累计结果
    val accumlatorResult = spark.sql(
      """
        |select aname as name,
        |       amounth as amount,
        |	      aamount as amount,
        |	      sum(bamount) accul_amount
        |from   temp_access_view
        |where  amounth>=bmounth
        |group  by aname,amounth,aamount
        |order  by aname,amounth
        |
      """.stripMargin)
    spark.stop()
  }
}
