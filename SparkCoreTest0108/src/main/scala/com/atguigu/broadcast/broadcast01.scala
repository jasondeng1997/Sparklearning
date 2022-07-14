package com.atguigu.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object broadcast01 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //3.创建一个字符串RDD，过滤出包含WARN的数据
    val rdd: RDD[String] = sc.makeRDD(List("WARN:Class Not Find", "INFO:Class Not Find", "DEBUG:Class Not Find"), 4)
    val str: String = "WARN"
    //创建广播变量
    val bdStr: Broadcast[String] = sc.broadcast(str)

    val frdd: RDD[String] = rdd.filter(
      log => {
        //使用广播变量
        log.contains(bdStr.value)
      }
    )

    frdd.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
