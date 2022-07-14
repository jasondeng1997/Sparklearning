package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value13_sortBy {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(7, 8, 1, 5, 3, 4, 9, 6, 2, 10))

    val rdd2: RDD[Int] = rdd.sortBy(x => -x)

    rdd2.collect().foreach(println)

    val strRDD: RDD[String] = sc.makeRDD(List("atguigu", "zoo", "hive", "banana", "spark", "hadoop"))

    val srdd: RDD[String] = strRDD.sortBy(word => word.charAt(0))

    val srdd2: RDD[String] = strRDD.sortBy(word => word)

    srdd2.collect().foreach(println)


    val numRDD: RDD[String] = sc.makeRDD(List("1111", "22", "34", "33"))
    val numRDD2: RDD[String] = numRDD.sortBy(num => num.toInt)

    numRDD2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
