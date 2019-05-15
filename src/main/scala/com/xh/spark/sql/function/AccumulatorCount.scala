package com.xh.spark.sql.function

import org.apache.spark.sql.SparkSession

/**
  * 累计汇总
  */
object AccumulatorCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateFrameFromJsonScala")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val usersDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "false")
      .load("src/main/resources/access.csv")
      .toDF("name", "mounth", "amount")

    /** 累加历史所有
      * DF 算子方式实现
      * rowsBetween(Long.MinValue, 0):窗口的大小是按照排序从最小值到当前行
      */
    // 方式一
    val accuCntSpec = Window.partitionBy("name").orderBy("mounth").rowsBetween(Long.MinValue, 0)
    usersDF.withColumn("acc_amount", sum(usersDF("amount")).over(accuCntSpec))

    // 方式二
    usersDF.select(
      $"name",
      $"mounth",
      $"amount",
      sum($"amount").over(accuCntSpec).as("acc_amount")
    )


    /** sql方式 */
    // 方式三
    usersDF.createOrReplaceTempView("access")
    spark.sql(
      """
        |select name,
        |       mounth,
        |       amount,
        |	      sum(amount) over (partition by name order by mounth asc  rows between unbounded preceding and current row ) as acc_amount
        |from   access
        |
      """.stripMargin)

    //累加N天之前,假设N=3
    // API方式
    val preThreeAccuCntSpec = Window.partitionBy("name").orderBy("mounth").rowsBetween(-3, 0)
    usersDF.select(
      $"name",
      $"mounth",
      $"amount",
      sum($"amount").over(preThreeAccuCntSpec).as("acc_amount"))

    // sql方式
    spark.sql(
      """
        |select name,
        |       mounth,
        |       amount,
        |	      sum(amount) over (partition by name order by mounth asc rows between 3 preceding and current row) as acc_amount
        |from   access
        |
      """.stripMargin)


    // 累加前3天，后3天
    // API方式
    val preThreeFiveAccuCntSpec = Window.partitionBy("name").orderBy("mounth").rowsBetween(3, 3)
    usersDF.select(
      $"name",
      $"mounth",
      $"amount",
      sum($"amount").over(preThreeFiveAccuCntSpec).as("acc_amount"))

    // sql方式
    spark.sql(
      """
        |select name,
        |       mounth,
        |       amount,
        |	      sum(amount) over (partition by name order by mounth asc rows between 3 preceding and 3 following) as acc_amount
        |from   access
        |
      """.stripMargin).show()
    spark.stop()
  }
}
