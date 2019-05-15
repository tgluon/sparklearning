package com.xh.spark.sql.practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 京东平台销售指标还原
  */
object JDSaleQuotaRevert {
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
    val tempJdVcpGoodsDayDF = spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", true)
      .option("header", true)
      .load("src/main/resources/temp_jd_vcp_goods_day.csv")
    // 指标表
    tempJdVcpGoodsDayDF.createOrReplaceTempView("temp_jd_vcp_goods_day")

    val tempProductDF = spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", true)
      .option("header", true)
      .load("src/main/resources/temp_product.csv")
    // 商品表
    tempProductDF.createOrReplaceTempView("temp_product")

    /** step1:所有商品销售指标按SKU(product_model)汇总（存在多个googs_id对应一个SKU的情况） */
    val jdGoodsDF = spark.sql(
      """
        |SELECT REGEXP_REPLACE(REGEXP_REPLACE(p.product_model, '^\\+', ''), '\\+$', '') AS product_model,
        |       p.category_second,
        |       to_date(q.ds, 'yyyymmdd') as ds,
        |       p.store_id,
        |       SUM(q.sale_num) AS sale_num,
        |       SUM(q.income) AS income,
        |       p.flag
        | FROM (
        |      -- 此处判断烟灶消或者大套系的前二个商品的组合是不是烟灶套餐
        |       SELECT v1.*,CASE WHEN v2.product_model IS NULL THEN '0' ELSE '1' END AS flag
        |       FROM  (select *
        |	          from  temp_product
        |			  where platform_ditch = 'jd' and category_second <> '烟灶'
        |			  )v1
        |            LEFT JOIN (
        |		     select *
        |			 from  temp_product
        |			 where platform_ditch = 'jd' and category_second = '烟灶'
        |			 )v2
        |            ON regexp_extract(v1.product_model, '^\\+[^+]*\\+[^+]*\\+',0) = v2.product_model
        |        union all
        |        select *,0 from  temp_product where platform_ditch = 'jd' and category_second = '烟灶'
        |      ) p
        | JOIN temp_jd_vcp_goods_day q
        |   ON p.goods_id = q.goods_id
        |	AND p.platform_ditch = 'jd'
        |	AND p.store_id = q.store_id
        |	AND q.ds >= nvl(p.start_time, TO_DATE('000010101', 'yyyymmdd'))
        |	AND q.ds < nvl(p.end_time, TO_DATE('99991231', 'yyyymmdd'))
        |GROUP BY REGEXP_REPLACE(REGEXP_REPLACE(p.product_model, '^\\+', ''), '\\+$', ''),
        |	       p.category_second,
        |	       q.ds,
        |	       p.store_id,
        |	       p.flag
      """.stripMargin)
    jdGoodsDF.createOrReplaceTempView("temp_jd_goods")


    spark.sql(
      """
        |	SELECT *,NAMED_STRUCT('sku', product_model, 'cate', category_second,'flag',flag)
        |	  FROM temp_jd_goods
        |	 WHERE category_second IN ('烟灶', '烟灶消', '大套系','嵌入式')
      """.stripMargin).show(1000000, false)
    spark.stop()
  }
}
