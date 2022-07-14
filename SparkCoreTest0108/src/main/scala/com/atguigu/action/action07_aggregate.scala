package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object action07_aggregate {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4),8)

    val sum: Int = rdd.aggregate(0)(_ + _, _ + _)
    val sum2: Int = rdd.fold(10)(_ + _)

    println(sum2)


    //TODO 3 关闭资源
    sc.stop()

  }

}
