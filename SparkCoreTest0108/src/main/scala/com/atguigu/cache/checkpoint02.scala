package com.atguigu.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object checkpoint02 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","atguigu")
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //做检查点之前要设置一下检查点存储的目录
    sc.setCheckpointDir("hdfs://hadoop102:8020/ck")

    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    val word2oneRDD: RDD[(String, Long)] = wordRDD.map(
      word => {
        (word, System.currentTimeMillis())
      }
    )

    //做检查点前查看血缘关系
    println(word2oneRDD.toDebugString)

    word2oneRDD.checkpoint()
    word2oneRDD.cache()


    word2oneRDD.collect().foreach(println)


    println("=========================")

    //做检查点后查看血缘关系
    println(word2oneRDD.toDebugString)

    word2oneRDD.collect().foreach(println)

    println("=========================")

    word2oneRDD.collect().foreach(println)

    //TODO 3 关闭资源
    sc.stop()

  }

}
