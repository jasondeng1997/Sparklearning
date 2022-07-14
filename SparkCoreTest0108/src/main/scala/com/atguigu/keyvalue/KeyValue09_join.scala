package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue09_join {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c"), (1, "d")))

    //3.2 创建第二个pairRDD
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6), (1, 7)))


    val rdd3: RDD[(Int, (String, Int))] = rdd1.join(rdd2)

    rdd3.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
