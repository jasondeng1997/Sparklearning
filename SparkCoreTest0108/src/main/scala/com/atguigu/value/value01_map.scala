package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value01_map {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4),2)

    val rdd2: RDD[Int] = rdd.map(_ * 2)


    val rdd3: RDD[Int] = rdd.map(_ + 1)

    rdd.map(x => -x).collect().foreach(println)

    val strRDD: RDD[String] = sc.makeRDD(List("atguigu", "zoo", "hive", "banana", "spark", "hadoop"))

    val srdd2: RDD[Char] = strRDD.map(word => word.charAt(0))

    srdd2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
