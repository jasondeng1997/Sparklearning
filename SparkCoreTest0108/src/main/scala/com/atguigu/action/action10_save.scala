package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object action10_save {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val strRDD: RDD[String] = sc.makeRDD(List("atguigu", "zoo", "hive", "banana", "spark", "hadoop"))

//    strRDD.coalesce(1).saveAsTextFile("D:\\IdeaProjects\\SparkCoreTest0108\\textout")
//    strRDD.coalesce(1).saveAsObjectFile("D:\\IdeaProjects\\SparkCoreTest0108\\objout")

    strRDD.coalesce(1).map((_,1)).saveAsSequenceFile("D:\\IdeaProjects\\SparkCoreTest0108\\seqout")

    //TODO 3 关闭资源
    sc.stop()

  }

}
