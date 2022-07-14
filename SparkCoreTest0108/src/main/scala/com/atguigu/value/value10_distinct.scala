package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value10_distinct {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val distinctRdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

    val rdd2: RDD[Int] = distinctRdd.distinct()

    rdd2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()
    4
  }

}
