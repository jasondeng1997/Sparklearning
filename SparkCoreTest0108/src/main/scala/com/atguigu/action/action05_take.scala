package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object action05_take {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(6, 7, 1, 4, 5, 2, 3, 8, 9, 10))


    val arr: Array[Int] = rdd.take(3)

    println(arr.mkString(","))

    val arr2: Array[Int] = rdd.takeOrdered(5)
    val arr3: Array[Int] = rdd.takeOrdered(5)(Ordering[Int].reverse)

    println(arr2.mkString(","))
    println(arr3.mkString(","))

    //TODO 3 关闭资源
    sc.stop()

  }

}
