package com.atguigu.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object DoubleValue01 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4)

    //3.2 创建第二个RDD
    val rdd2: RDD[Int] = sc.makeRDD(4 to 8)

    //交集
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)

    rdd3.collect().foreach(println)

    //并集 (不去重)
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    rdd4.collect().foreach(println)

    // 差集
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    rdd5.collect().foreach(println)

    val rdd6: RDD[Int] = rdd2.subtract(rdd1)
    rdd6.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
