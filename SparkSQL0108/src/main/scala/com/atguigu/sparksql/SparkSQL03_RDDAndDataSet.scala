package com.atguigu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author layne
 */
object SparkSQL03_RDDAndDataSet {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //转换之前,导入隐式转换
    import spark.implicits._

    //rdd =>  DS
    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkSQL0108\\input\\user.txt")

    val rdd2: RDD[(String, String)] = rdd.map(
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), datas(1))
      }
    )

    //普通rdd转换ds,不能补充列名(元数据),因此不推荐
    val ds: Dataset[(String, String)] = rdd2.toDS()

    ds.show()



    val userRDD: RDD[User] = rdd2.map {
      case (name, age) => User(name, age.toLong)
    }

    //样例类RDD转DS,推荐使用
    val userDS: Dataset[User] = userRDD.toDS()

    userDS.show()

    //DS => RDD

    //DS转RDD直接.rdd即可,并且DS会保留原始数据的类型,这个是比DF要强大的
    val userRDD2: RDD[User] = userDS.rdd
    val rdd1: RDD[(String, String)] = ds.rdd




    //TODO 3 关闭资源
    spark.stop()

  }

}

