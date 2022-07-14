package com.atguigu.readAndSave

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object Operate_Text {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //text文件的读取
    val rdd: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    rdd.collect().foreach(println)

    //text文件的存储

    val rdd2: RDD[String] = rdd.filter(_.contains("spark"))

    rdd2.coalesce(1).saveAsTextFile("D:\\IdeaProjects\\SparkCoreTest0108\\outtext")



    //TODO 3 关闭资源
    sc.stop()

  }

}
