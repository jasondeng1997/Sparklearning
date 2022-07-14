package com.atguigu.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object cache01 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    val word2oneRDD: RDD[(String, Int)] = wordRDD.map(
      word => {
        println("****************")
        (word, 1)
      }
    )

    //缓存前查看血缘关系
    println(word2oneRDD.toDebugString)

    //因为word2oneRDD在下面需要用到多次,因此最好提前缓存一下
//    word2oneRDD.cache()
    word2oneRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    word2oneRDD.collect().foreach(println)


    println("=========================")

    //缓存后查看血缘关系
    println(word2oneRDD.toDebugString)

    word2oneRDD.collect().foreach(println)

    //释放缓存
    word2oneRDD.unpersist()




    //TODO 3 关闭资源
    sc.stop()

  }

}
