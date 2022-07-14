package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object action09_countByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "d"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))

    val intToLong: collection.Map[Int, Long] = rdd.countByKey()

    println(intToLong)

    val tupleToLong: collection.Map[(Int, String), Long] = rdd.countByValue()

    println(tupleToLong)


    //TODO 3 关闭资源
    sc.stop()

  }

}
