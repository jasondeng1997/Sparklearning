package com.atguigu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author layne
 */
object SparkSQL02_RDDAndDataFrame {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //转换之前,导入隐式转换
    import spark.implicits._

    //rdd =>  DF
    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkSQL0108\\input\\user.txt")

    val rdd2: RDD[(String, String)] = rdd.map(
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), datas(1))
      }
    )

    //普通rdd转换df,需要补充列名(元数据)
    val df: DataFrame = rdd2.toDF("name","age")

    df.show()

    val userRDD: RDD[User] = rdd2.map {
      case (name, age) => User(name, age.toLong)
    }

    //样例类RDD转DF,df会取样例类的属性名作为列名,推荐使用
    val userDF: DataFrame = userRDD.toDF()

    userDF.show()

    //DF => RDD

    //df转rdd直接.rdd即可,但是df会丢掉原本数据的类型,统一修改为row
    val rdd1: RDD[Row] = df.rdd
    val userRDD2: RDD[Row] = userDF.rdd

    val userRDD3: RDD[User] = userRDD2.map(
      row => User(row.getString(0), row.getLong(1))
    )





    //TODO 3 关闭资源
    spark.stop()

  }

}

case class User(name:String,age:Long)
