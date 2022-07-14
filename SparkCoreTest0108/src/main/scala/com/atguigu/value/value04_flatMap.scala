package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value04_flatMap {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val listRDD=sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7)), 2)

    val rdd2: RDD[Int] = listRDD.flatMap(list => list.map(_ * 2))

//    rdd2.collect().foreach(println)

    val strRDD: RDD[String] = sc.makeRDD(List("atguigu hello spark", "zoo hive spark"))

    val wordRDD: RDD[String] = strRDD.flatMap(_.split(" "))

    wordRDD.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
