package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object value07_groupby_wordcount {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    val word2oneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    val groupRDD: RDD[(String, Iterable[(String, Int)])] = word2oneRDD.groupBy(_._1)

    groupRDD.collect().foreach(println)

    //1 lambda表达式写法
    val resRDD: RDD[(String, Int)] = groupRDD.map (
      t => (t._1, t._2.size)
    )

    resRDD.collect().foreach(println)


    //2 偏函数写法
    val resRDD2: RDD[(String, Int)] = groupRDD.map {
      case (word, iter) => (word, iter.size)
    }

    resRDD2.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
