package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object require01_top10Category_method1_2 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1.读取数据
    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\user_visit_action.txt")

    lineRDD.cache()
    //2.过滤出所有点击的数据
    val clickDataRDD: RDD[String] = lineRDD.filter(
      line => {
        val datas: Array[String] = line.split("_")
        datas(6) != "-1"
      }
    )

    //3.转换数据结构 (品类,点击1) => (品类,点击总数)
    val clickCountRDD: RDD[(String, Int)] = clickDataRDD.map(
      line => {
        val datas: Array[String] = line.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    //4.过滤出所有下单的数据
    val orderDataRDD: RDD[String] = lineRDD.filter(
      line => {
        val datas: Array[String] = line.split("_")
        datas(8) != "null"
      }
    )

    //5 求出(品类,下单总数)
    val orderCountRDD: RDD[(String, Int)] = orderDataRDD.flatMap(
      line => {
        val datas: Array[String] = line.split("_")
        val ids: Array[String] = datas(8).split(",")
        ids.map((_, 1))
      }
    ).reduceByKey(_ + _)

    //6 过滤出所有支付的数据
    val payDataRDD: RDD[String] = lineRDD.filter(
      line => {
        val datas: Array[String] = line.split("_")
        datas(10) != "null"
      }
    )

    //7 计算(品类,支付总数)
    val payCountRDD: RDD[(String, Int)] = payDataRDD.flatMap(
      line => {
        val datas: Array[String] = line.split("_")
        val ids: Array[String] = datas(10).split(",")
        ids.map((_, 1))
      }
    ).reduceByKey(_ + _)

    //8  使用union补0的方式实现满外连
    // (品类id,点击数) => (品类id,(点击数,0,0))
    // (品类id,下单数) => (品类id,(0,下单数,0))
    // (品类id,支付数) => (品类id,(0,0,支付数))
    //  (品类id,(点击数,下单数,支付数))

    val clickRDD: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (id, cnt) => (id, (cnt, 0, 0))
    }

    val orderRDD: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (id, cnt) => (id, (0, cnt, 0))
    }

    val payRDD: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (id, cnt) => (id, (0, 0, cnt))
    }

    val unionRDD: RDD[(String, (Int, Int, Int))] = clickRDD.union(orderRDD).union(payRDD)

    //9 按照相同的品类id聚合三元组
    val unionRDD2: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey(
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    )

    //10 倒序排序取前10
    unionRDD2.sortBy(_._2,false).take(10).foreach(println)







    //TODO 3 关闭资源
    sc.stop()

  }

}
