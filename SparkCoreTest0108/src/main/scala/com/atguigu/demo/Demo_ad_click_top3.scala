package com.atguigu.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object Demo_ad_click_top3 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //1 读取文件  1516609143867 6 7 64 16
    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\agent.log")

    //2 转变数据结构 1516609143867 6 7 64 16 => (6-16,1)  (省份-广告,1)
    val proAndAdv2oneRDD: RDD[(String, Int)] = lineRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    )

    //3 按照key聚合Value,求出每个省份每个广告的总点击次数
    val proAndAdv2SumRDD: RDD[(String, Int)] = proAndAdv2oneRDD.reduceByKey(_ + _)

    //4 再次转换数据结构 (6-16,sum)  =>  (6,(16,sum))  (省份-广告,sum) => (省份,(广告,点击次数))
    val pro2AdvAndSumRDD: RDD[(String, (String, Int))] = proAndAdv2SumRDD.map {
      case (proAndAdv, sum) => {
        val ks: Array[String] = proAndAdv.split("-")
        (ks(0), (ks(1), sum))
      }
    }

    //5 既然咱们掰开了省份作为了key,因此要按照省份分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = pro2AdvAndSumRDD.groupByKey()

    //6 对每个省份的所有的广告按照点击次数倒序排序,取前3
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering[Int].reverse).take(3)
      }
    )

    resRDD.collect().foreach(println)

    println("=========================")

    val resRDD2: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (t1, t2) => t1._2 > t2._2
        ).take(3)
      }
    )

    resRDD2.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}
