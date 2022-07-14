package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object partition04_file {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\3.txt",3)

    rdd.saveAsTextFile("D:\\IdeaProjects\\SparkCoreTest0108\\fileout4")

    /**
     * spark读取文件 用的是hadoop1.x那套API
     * 按行读取
     */



    //TODO 3 关闭资源
    sc.stop()

  }

}
