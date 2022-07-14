package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value11_coalesce {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)

    val rdd2: RDD[Int] = rdd.coalesce(2)

    rdd2.mapPartitionsWithIndex(
          (index,iters)=>iters.map((index,_))
        ).collect().foreach(println)

    println("-------------------")

    val rdd3: RDD[Int] = rdd.coalesce(4,true)

    rdd3.mapPartitionsWithIndex(
          (index,iters)=>iters.map((index,_))
        ).collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
