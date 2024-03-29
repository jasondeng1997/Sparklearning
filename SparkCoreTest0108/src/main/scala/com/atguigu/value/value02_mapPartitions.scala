package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value02_mapPartitions {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4),2)


    val rdd2: RDD[Int] = rdd.mapPartitions(iter => iter.map(x => x * 2))
    val rdd3: RDD[Int] = rdd.mapPartitions(_.map(_ * 2))


    rdd2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
