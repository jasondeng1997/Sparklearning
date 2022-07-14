package com.atguigu.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object Lineage03 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)  //1 个 application

    //stage0有2个task
    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")
    val flatRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    //2个stage (因为reduceByKey算子有宽依赖,stage = 宽依赖的个数 + 1)
    //stage1有2个task
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)


    //2个job
    resultRDD.collect().foreach(println)
    resultRDD.saveAsTextFile("D:\\IdeaProjects\\SparkCoreTest0108\\output")


    Thread.sleep(Long.MaxValue)

    //TODO 3 关闭资源
    sc.stop()

  }

}
