package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author layne
 */
object SparkSQL08_Load {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")

    conf.set("spark.sql.sources.default","csv")

    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //特定方式加载
    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0108\\input\\user.json")
    val df2: DataFrame = spark.read.csv("D:\\IdeaProjects\\SparkSQL0108\\input\\user.txt")


    df.show()
    df2.show()

    //通用方式加载
    val df3: DataFrame = spark.read.load("D:\\IdeaProjects\\SparkSQL0108\\input\\user.txt")

    df3.show()


    //TODO 3 关闭资源
    spark.stop()
  }

}
