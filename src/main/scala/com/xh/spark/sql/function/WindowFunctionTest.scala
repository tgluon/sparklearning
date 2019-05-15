package com.xh.spark.sql.function

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WindowFunctionTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("WindowFunctionTest")
          .set("spark.master", "yarn")
//          .set("spark.local.ip", "192.168.8.84") // idea 所在机器IP
//          .set("spark.driver.host", "hadoop04")
          .set("spark.submit.deployMode", "client") // 部署模式为client
          .set("yarn.resourcemanager.hostname", "hadoop02") // resourcemanager主机名
          .set("spark.executor.instances", "2") // Executor实例的数量
          .set("spark.dynamicAllocation.enabled", "false")
          .setJars(List("/home/hadoop/worker/sparklearning/target/sparklearning-1.0-SNAPSHOT.jar",
            "/home/hadoop/worker/sparklearning/target/sparklearning-1.0-SNAPSHOT-jar-with-dependencies.jar"
          ))


    val spark = SparkSession
      .builder()
//      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df = List(
      ("站点1", "2018-01-01", 50),
      ("站点1", "2018-01-02", 45),
      ("站点1", "2018-01-03", 55),
      ("站点2", "2018-01-01", 25),
      ("站点2", "2018-01-02", 29),
      ("站点2", "2018-01-03", 27)
    ).toDF("site", "date", "user_cnt")

    df.createOrReplaceTempView("site_info")
    // 简单移动平均值
    // API方式
    // 窗口定义从 -1(前一行)到 1(后一行)	，每一个滑动的窗口总用有3行
//    val movinAvgSpec = Window.partitionBy("site").orderBy("date").rowsBetween(-1, 1)
//    df.withColumn("MovingAvg", avg(df("user_cnt")).over(movinAvgSpec)).show()

    // sql方式
    //    spark.sql(
    //      """
    //        |select site,
    //        |       date,
    //        |       user_cnt,
    //        |       avg(user_cnt) over(partition by site order by date rows between 1 preceding and 1 following) as moving_avg
    //        |from   site_info
    //      """.stripMargin)
    //
    //
    //    // 前一行数据
    //    // API方式
    //    val lagwSpec = Window.partitionBy("site").orderBy("date")
    //    df.withColumn("prevUserCnt", lag(df("user_cnt"), 1).over(lagwSpec))
    //
    //    // sql方式
    //    spark.sql(
    //      """
    //        |select site,
    //        |       date,
    //        |       user_cnt,
    //        |       lag(user_cnt,1) over(partition by  site order by date asc ) as prevUserCnt
    //        |from   site_info
    //      """.stripMargin)
    //
    //
    //    // 排名
    //    // API方式
    //    val rankwSpec = Window.partitionBy("site").orderBy("date")
    //    df.withColumn("rank", rank().over(rankwSpec))
    //
    //    // sql方式
    //    spark.sql(
    //      """
    //        |select site,
    //        |       date,
    //        |       user_cnt,
    //        |       rank() over(partition by  site order by date asc ) as prevUserCnt
    //        |from   site_info
    //      """.stripMargin)
    spark.stop()
  }
}
