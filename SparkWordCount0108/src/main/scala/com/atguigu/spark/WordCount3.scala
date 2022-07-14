package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object WordCount3 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建Spark配置文件
    val conf = new SparkConf().setAppName("SparkWordCount")//.setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1 读取文件
    val lineRDD: RDD[String] = sc.textFile(args(0))
    //2 按照空格切割数据,形成一个个的单词
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    //3 转换数据结构  hello => (hello,1)
    val word2oneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    //4 按照相同单词,计数
    val resultRDD: RDD[(String, Int)] = word2oneRDD.reduceByKey(_ + _)
    //5 将结果保存到文件中
    resultRDD.saveAsTextFile(args(1))


    //TODO 3 关闭资源
    sc.stop()

  }
}
