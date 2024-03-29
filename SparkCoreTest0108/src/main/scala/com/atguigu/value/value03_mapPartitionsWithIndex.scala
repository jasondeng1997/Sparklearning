package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value03_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4),2)

    val rdd2: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, iter) => iter.map(x => (index, x)))

    rdd2.collect().foreach(println)


    val rdd4: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),6)

    rdd4.mapPartitionsWithIndex(
          (index,iters)=>iters.map((index,_))
        ).collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
