package com.atguigu.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object Demo2_wc_agg {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    val resultRDD: RDD[(String, Int)] = wordRDD.map((_, 1)).aggregateByKey(0)(_ + _, _ + _)
    val resultRDD2: RDD[(String, Int)] = wordRDD.map((_, 1)).foldByKey(0)(_ + _)

    resultRDD.collect().foreach(println)

    resultRDD2.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
