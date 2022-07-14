package com.atguigu.readAndSave

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object Operate_Sequence {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val strRDD: RDD[String] = sc.makeRDD(List("atguigu", "zoo", "hive", "banana", "spark", "hadoop"))

    //seq文件的存储
//    strRDD.map((_,1)).coalesce(1).saveAsSequenceFile("D:\\IdeaProjects\\SparkCoreTest0108\\outseq")

    //seq文件的读取
    val rdd: RDD[(String, Int)] = sc.sequenceFile[String, Int]("D:\\IdeaProjects\\SparkCoreTest0108\\outseq")

    rdd.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()
  }


}
