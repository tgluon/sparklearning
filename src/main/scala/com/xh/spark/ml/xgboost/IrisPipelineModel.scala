package com.xh.spark.ml.xgboost

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object IrisPipelineModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("IrisPipelineModel")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType(Array(
      StructField("sepal length", DoubleType, true),
      StructField("sepal width", DoubleType, true),
      StructField("petal length", DoubleType, true),
      StructField("petal width", DoubleType, true),
      StructField("class", StringType, nullable = true)))


    val rawInput = spark.read.schema(schema).csv("src/main/resources/data/iris.csv")

    val Array(training, test) = rawInput.randomSplit(Array(0.8, 0.2), 123)

    val labelIndexer = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("classIndex")
      .fit(training)
    val labelTransform = labelIndexer.transform(rawInput).drop("class")


    val assembler = new VectorAssembler()
      .setInputCols(Array("sepal length", "sepal width", "petal length", "petal width"))
      .setOutputCol("features")

    spark.stop()
  }
}
