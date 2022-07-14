package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue05_foldByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    val list: List[(String, Int)] = List(("a",1),("a",3),("a",5),("b",7),("b",2),("b",4),("b",6),("a",7))
    val rdd = sc.makeRDD(list,2)

//    val foldRDD: RDD[(String, Int)] = rdd.foldByKey(0)(Math.max)
    val foldRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)

    foldRDD.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
