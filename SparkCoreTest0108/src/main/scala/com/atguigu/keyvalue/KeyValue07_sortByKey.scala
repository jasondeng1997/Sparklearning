package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue07_sortByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    val rdd2: RDD[(Int, String)] = rdd.sortByKey()

    rdd2.collect().foreach(println)

    val rdd3: RDD[(Int, String)] = rdd.sortBy(_._1)

    rdd3.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
