package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author layne
 */
object SparkSQL05_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0108\\input\\user.json")

    df.createOrReplaceTempView("user")

    spark.sql("select * from user").show()

    spark.udf.register("addName",(name:String)=>{ "Name:" + name})
    spark.udf.register("doubleAge",(age:Long)=>{ age * 2})


    spark.sql("select doubleAge(age),addName(name) from user").show()

    //TODO 3 关闭资源
    spark.stop()
  }

}
