package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object action03_count {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(10000, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val cnt: Long = rdd.count()

    println(cnt)


    val int: Int = rdd.first()

    println(int)


    //TODO 3 关闭资源
    sc.stop()

  }

}
