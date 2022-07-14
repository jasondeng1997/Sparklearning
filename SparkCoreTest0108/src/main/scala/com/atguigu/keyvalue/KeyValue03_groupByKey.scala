package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue03_groupByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    groupRDD.collect().foreach(println)
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (k, v) => (k, v.sum)
    }

    resRDD.collect().foreach(println)


    println("====================")

    //========================跟groupBY的区别====================================
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    groupRDD2.collect().foreach(println)

    val resRDD2: RDD[(String, Int)] = groupRDD2.map {
      case (k, v) => (k, v.map(_._2).sum)
    }

    resRDD2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
