package com.atguigu.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object Demo1_wc_group {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    //====================groupBy算子实现wc======================
/*    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word => word)

    val resultRDD: RDD[(String, Int)] = groupRDD.map {
      case (k, v) => (k, v.toList.size)
    }

    resultRDD.collect().foreach(println)*/

    val groupRDD: RDD[(String, Iterable[Int])] = wordRDD.map((_, 1)).groupByKey()

    val resultRDD: RDD[(String, Int)] = groupRDD.map(
      t => (t._1, t._2.sum)
    )

    resultRDD.collect().foreach(println)






    //TODO 3 关闭资源
    sc.stop()

  }

}
