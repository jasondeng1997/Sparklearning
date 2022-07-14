package com.atguigu.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object Lineage01 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    println(lineRDD.toDebugString)
    println("------------------")


    val flatRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    println(flatRDD.toDebugString)
    println("------------------")


    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    println(mapRDD.toDebugString)
    println("------------------")


    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    println(resultRDD.toDebugString)
    println("------------------")


    resultRDD.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
