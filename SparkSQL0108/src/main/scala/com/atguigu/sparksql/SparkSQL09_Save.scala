package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author layne
 */
object SparkSQL09_Save {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")

    conf.set("spark.sql.sources.default","csv")

    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0108\\input\\user.json")

    df.show()

    //特定方式写出
//    df.write.json("D:\\IdeaProjects\\SparkSQL0108\\out")

    //通用方式写出
    df.write.mode(SaveMode.Ignore).save("D:\\IdeaProjects\\SparkSQL0108\\out4")

    /**
     * 写出模式
     * 1.默认模式(存在即报错) 如果目录不存在,正常写出;如果目录存在,报错.
     * 2.追加模式(追加写出) 如果目录不存在,正常写出;如果目录存在,追加写出.
     * 3.覆盖模式(覆盖写出) 如果目录不存在,正常写出;如果目录存在,删除已有的目录,新建一个同名目录,写出(慎用)
     * 4.忽略模式(忽略写出) 如果目录不存在,正常写出;如果目录存在,忽略本次操作,不报错
     */


    //TODO 3 关闭资源
    spark.stop()
  }

}
