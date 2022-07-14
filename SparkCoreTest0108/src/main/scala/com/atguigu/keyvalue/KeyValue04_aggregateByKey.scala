package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue04_aggregateByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",3),("a",5),("b",7),("b",2),("b",4),("b",6),("a",7)), 2)


//    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(Math.max, _ + _)
//    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(10)(Math.max, _ + _)
//    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(Math.max,Math.max)
    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(10)(_ + _,_ + _)

    aggRDD.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
