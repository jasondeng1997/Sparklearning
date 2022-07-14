package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object SparkSQL10_MySQL_Read {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()

    df.createOrReplaceTempView("user")


    spark.sql("select gender,count(*) from user group by gender").show(500,false)






    //TODO 3 关闭资源
    spark.stop()


  }

}
