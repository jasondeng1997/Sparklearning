package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object partition01_Array_default {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    rdd.saveAsTextFile("D:\\IdeaProjects\\SparkCoreTest0108\\out3")

    /**
     * 从集合中创建rdd,默认分区数  为程序的总核心数
     * 本地模式:scheduler.conf.getInt("spark.default.parallelism", totalCores)
     * 集群模式:conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
     */



    //TODO 3 关闭资源
    sc.stop()

  }

}
