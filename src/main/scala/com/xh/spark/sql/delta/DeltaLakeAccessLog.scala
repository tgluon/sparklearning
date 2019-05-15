package com.xh.spark.sql.delta

import org.apache.spark.sql.SparkSession

object DeltaLakeAccessLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateFrameFromJsonScala")
      .config("spark.some.config.option", "some-value")
      .enableHiveSupport()
      .getOrCreate()

    val usersDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "false")
      .load("/data/access.csv")
      .toDF("name", "mounth", "amount")
    val result = usersDF.selectExpr("name", "mounth", "cast(amount as double) amount")

    result.write.format("delta")
      .mode("overwrite")
      .save("/hive/warehouse/ods.db/delta")

    spark.read.format("delta")
      .option("versionAsOf", 0)
      .load("/hive/warehouse/ods.db/delta")
      .show(1000)
    spark.stop()
  }
}
