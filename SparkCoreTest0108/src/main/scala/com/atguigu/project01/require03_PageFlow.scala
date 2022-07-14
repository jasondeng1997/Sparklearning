package com.atguigu.project01

import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
 * @author layne
 */
object require03_PageFlow {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //1.读取数据
    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkCoreTest0108\\input\\user_visit_action.txt")

    //2 用样例类封装数据
    val actionRDD: RDD[UserVisitAction] = lineRDD.map(
      line => {
        val datas: Array[String] = line.split("_")
        UserVisitAction(
          datas(0),
          datas(1),
          datas(2),
          datas(3),
          datas(4),
          datas(5),
          datas(6),
          datas(7),
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12)
        )
      }
    )

    //3 定义过滤集合
    val ids = List("1", "2", "3", "4", "5", "6", "7")
    val zipIds: List[String] = ids.zip(ids.tail).map {
      case (p1, p2) => p1 + "-" + p2
    }

    val bdIds: Broadcast[List[String]] = sc.broadcast(ids)
    val bdZipIds: Broadcast[List[String]] = sc.broadcast(zipIds)

    //4 求分母
    val fmRDD: RDD[(String, Int)] = actionRDD
      .filter(action => bdIds.value.init.contains(action.page_id))
      .map(action => (action.page_id, 1))
      .reduceByKey(_ + _)

    val idsArr: Array[(String, Int)] = fmRDD.collect()

    val idsMap: Map[String, Int] = idsArr.toMap


    //5 求分子
    //5.1 先按照会话id对数据进行分组
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    // 5.2 对每个分组下的数据做处理
    val filterRDD: RDD[List[String]] = sessionGroupRDD.mapValues(
      iter => {
        // 1 先对每个分组的页面数据 严格按照发生时间排序
        val orderList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

        //2 改变数据结构
        val pageIdList: List[String] = orderList.map(_.page_id)

        //3 pageIdList和自己的尾巴拉链 形成页面单跳
        val zipList: List[(String, String)] = pageIdList.zip(pageIdList.tail)

        //4 转换数据结构 形成单跳字符串 (1,2) => 1-2
        val pageJumpList: List[String] = zipList.map {
          case (p1, p2) => p1 + "-" + p2
        }
        //5 过滤数据 只要 1-2 2-3 3-4 4-5 5-6 6-7的数据
        pageJumpList.filter(
          page2page => bdZipIds.value.contains(page2page)
        )
      }
    ).map(_._2)

    //5.3 拍平每个会话下的多个pageJump
    val resRDD: RDD[(String, Int)] = filterRDD.flatMap(list => list).map((_, 1)).reduceByKey(_ + _)

    println("======================分母===========================")
    println(idsMap)
    println("======================分子===========================")
    resRDD.collect().foreach(println)

    println("======================页面单跳转换率===========================")

    resRDD.foreach {
      case (page2page, cnt) => {
        val pages: Array[String] = page2page.split("-")

        val prePageCnt: Int = idsMap(pages(0))

        println(page2page + "=" + cnt.toDouble / prePageCnt)
      }
    }

    //TODO 3 关闭资源
    sc.stop()

  }

}
