package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object createrdd01_array {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建spark配置文件
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 创建SC对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(1 to 5)
    val rdd2: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6))
    val rdd3: RDD[Int] = sc.parallelize(List(1,2,3,4,5))

    rdd3.collect().foreach(println)

    println(rdd2.collect().mkString(","))

    val rdd4: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6))


    //TODO 3 关闭资源
    sc.stop()

  }
}
