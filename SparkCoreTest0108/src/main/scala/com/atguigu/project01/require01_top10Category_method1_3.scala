package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object require01_top10Category_method1_3 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1.读取数据
    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\user_visit_action.txt")

    //2.转换数据结构
    val dataRDD: RDD[(String, (Int, Int, Int))] = lineRDD.flatMap(
      line => {
        val datas: Array[String] = line.split("_")
        if (datas(6) != "-1") {
          //点击的数据
          List((datas(6), (1, 0, 0)))

        } else if (datas(8) != "null") {
          //下单的数据
          val ids: Array[String] = datas(8).split(",")

          ids.map((_, (0, 1, 0)))
        } else if (datas(10) != "null") {
          //支付的数据
          val ids: Array[String] = datas(10).split(",")

          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    //3 按照相同的品类id 聚合三元组
    val reduceRDD: RDD[(String, (Int, Int, Int))] = dataRDD.reduceByKey(
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    )

    //4 倒序排序取前10
    reduceRDD.sortBy(_._2,false).take(10).foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}
