package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue02_reduceByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)),2)

    val rdd2: RDD[(String, Int)] = rdd.reduceByKey((v1, v2) => v1 + v2)

    rdd2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
