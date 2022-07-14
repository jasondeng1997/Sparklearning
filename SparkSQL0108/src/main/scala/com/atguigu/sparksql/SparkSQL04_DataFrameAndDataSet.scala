package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author layne
 */
object SparkSQL04_DataFrameAndDataSet {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //df 跟 ds相互转换也需要导入隐式转换
    import spark.implicits._

    //DF => DS
    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0108\\input\\user.json")

    df.show()

    //df修改列名
    val df2: DataFrame = df.toDF("age2", "name2")

    println("----------------")

    df2.show()

    val ds: Dataset[User] = df.as[User]


    println("================")
    ds.show()


    //DS => DF
    val df22: DataFrame = ds.toDF()

    df22.show()



    //TODO 3 关闭资源
    spark.stop()
  }

}
