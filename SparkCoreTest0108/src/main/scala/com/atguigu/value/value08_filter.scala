package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value08_filter {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val filterRDD: RDD[Int] = rdd.filter(_ % 2 == 0)

//    filterRDD.collect().foreach(println)

    val frdd: RDD[Int] = rdd.filter(_ > 5)

    frdd.collect().foreach(println)

    val strRDD: RDD[String] = sc.makeRDD(List("atguigu", "zoo", "hive", "banana", "spark", "hadoop"))

    val strRDD2: RDD[String] = strRDD.filter(_.startsWith("h"))

    strRDD2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
