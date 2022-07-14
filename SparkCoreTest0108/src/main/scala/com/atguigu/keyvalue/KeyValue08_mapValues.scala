package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue08_mapValues {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "atguigu"), (1, "driver"), (2, "black"), (3, "color")))

    val rdd2: RDD[(Int, Char)] = rdd.map {
      case (k, v) => (k, v.charAt(0))
    }

    rdd2.collect().foreach(println)


    val rdd3: RDD[(Int, Char)] = rdd.mapValues(_.charAt(0))

    rdd3.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
