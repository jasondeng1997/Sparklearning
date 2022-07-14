package com.atguigu.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object cache02 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    val word2oneRDD: RDD[(String, Int)] = wordRDD.map(
      word => {
        println("****************")
        (word, 1)
      }
    )

    val resultRDD: RDD[(String, Int)] = word2oneRDD.reduceByKey(_ + _)


    resultRDD.collect().foreach(println)


    println("=========================")


    resultRDD.collect().foreach(println)


    Thread.sleep(Long.MaxValue)




    //TODO 3 关闭资源
    sc.stop()

  }

}
