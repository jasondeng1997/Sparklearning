package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object partition03_file_default {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    rdd.saveAsTextFile("D:\\IdeaProjects\\SparkCoreTest0108\\fileout1")

    /**
     * 读取文件创建RDD
     * 默认分区数是 程序总核心数 和 2 取最小值  一般都是2
     * def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
     */


    //TODO 3 关闭资源
    sc.stop()

  }

}
