package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue06_combineByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //第一个参数: 88 => (88,1)
    //第二个参数: (88,1) 91 => (88+91,1+1)
    //第三个参数: (179,2) (95,1) => (179 + 95,2+1)
    val combinRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      num => (num, 1),
      (t: (Int, Int), num) => (t._1 + num, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )

    combinRDD.collect().foreach(println)

    val resRDD: RDD[(String, Double)] = combinRDD.map {
      case (k, v) => (k, v._1.toDouble / v._2)
    }

    resRDD.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
