package com.atguigu.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author layne
 */
object partitioner01_get {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val strRDD: RDD[String] = sc.makeRDD(List("atguigu", "zoo", "hive", "banana", "spark", "hadoop"))

    println(strRDD.partitioner)

    val mapRDD: RDD[(String, Int)] = strRDD.map((_, 1))

    println(mapRDD.partitioner)

    val hashRDD: RDD[(String, Int)] = mapRDD.partitionBy(new HashPartitioner(2))

    println(hashRDD.partitioner)


    //TODO 3 关闭资源
    sc.stop()

  }

}
