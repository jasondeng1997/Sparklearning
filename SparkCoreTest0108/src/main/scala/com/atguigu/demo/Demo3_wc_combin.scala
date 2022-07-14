package com.atguigu.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object Demo3_wc_combin {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    val resRDD: RDD[(String, Int)] = wordRDD.map((_, 1)).combineByKey(num => num, (x: Int, y:Int) => x + y, (x: Int, y: Int) => x + y)


    resRDD.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()
  }

}
