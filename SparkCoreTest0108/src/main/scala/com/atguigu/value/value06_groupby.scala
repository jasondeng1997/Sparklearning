package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value06_groupby {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(x => x % 2)

    val groupRDD2= rdd.groupBy(x => if (x <= 5) 0 else 1 )

    groupRDD2.collect().foreach(println)

    groupRDD.collect().foreach(println)


    val strRDD: RDD[String] = sc.makeRDD(List("atguigu", "zoo", "hive", "anana", "spark", "hadoop"))

    val srdd2: RDD[(Char, Iterable[String])] = strRDD.groupBy(word => word.charAt(0))

    srdd2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
