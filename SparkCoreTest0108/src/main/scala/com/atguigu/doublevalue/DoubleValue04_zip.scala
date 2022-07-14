package com.atguigu.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object DoubleValue04_zip {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3,4),2)


    //3.2 创建第二个RDD
    val rdd2: RDD[String] = sc.makeRDD(Array("a","b","c","d"),2)

    val zipRDD: RDD[(Int, String)] = rdd1.zip(rdd2)

    zipRDD.collect().foreach(println)


    /**
     * 结论 spark的zip功能跟scala集合的zip一样
     * 但是要求更严格  1.元素个数一样  2.分区个数一样
     */



    //TODO 3 关闭资源
    sc.stop()

  }

}
