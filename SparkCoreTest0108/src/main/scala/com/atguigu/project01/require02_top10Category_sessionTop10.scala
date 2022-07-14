package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object require02_top10Category_sessionTop10 {
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

    //3 转换数据结构
    val infoRDD = actionRDD.flatMap(
      action => {
        if (action.click_category_id != "-1") {
          //点击的数据
          List((action.click_category_id, CategoryCountInfo(action.click_category_id, 1, 0, 0)))
        } else if (action.order_category_ids != "null") {
          //下单的数据
          val ids: Array[String] = action.order_category_ids.split(",")

          ids.map(id => (id, CategoryCountInfo(id, 0, 1, 0)))

        } else if (action.pay_category_ids != "null") {
          //支付的数据
          val ids: Array[String] = action.pay_category_ids.split(",")

          ids.map(id => (id, CategoryCountInfo(id, 0, 0, 1)))

        } else {
          Nil
        }
      }
    )

    //4 按照品类id聚合info
    val reduceRDD = infoRDD.reduceByKey(
      (info1, info2) => {
        info1.clickCount += info2.clickCount
        info1.orderCount += info2.orderCount
        info1.payCount += info2.payCount
        info1
      }
    ).map(_._2)


    //6 倒序排序取前10
    val top10infoArr: Array[CategoryCountInfo] = reduceRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)

    //**************************需求2************************************
    //1 转换数据结构
    val top10Ids: Array[String] = top10infoArr.map(_.categoryId)

    //2 过滤数据(过滤出热门top10品类的点击数据)(因为咱们默认会话是从点击开始的)
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(
      action => {
        if (action.click_category_id != "-1") {
          top10Ids.contains(action.click_category_id)
        } else {
          false
        }
      }
    )

    //3 转换数据结构 UserVisitAction => (品类id = 会话id,1)
    val catAndsess2oneRDD: RDD[(String, Int)] = filterRDD.map(
      action => (action.click_category_id + "=" + action.session_id, 1)
    )

    //4 求出(品类id = 会话id,1) => (品类id = 会话id,sum)
    val catAndsess2sumRDD: RDD[(String, Int)] = catAndsess2oneRDD.reduceByKey(_ + _)

    //5 再次转换数据结构 (品类id = 会话id,sum) => (品类id,(会话id,sum))
    val cat2sessAndsumRDD: RDD[(String, (String, Int))] = catAndsess2sumRDD.map {
      case (catAndsess, sum) => {
        val ks: Array[String] = catAndsess.split("=")
        (ks(0), (ks(1), sum))
      }
    }

    //6 按照品类id分组 求出每个分组下的所有的会话id 和 会话点击次数
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = cat2sessAndsumRDD.groupByKey()

    //7 对每个品类分组下的会话id 按照点击次数 倒序排序取前10
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering[Int].reverse).take(10)
      }
    )

    resRDD.collect().foreach(println)



    //TODO 3 关闭资源
    sc.stop()

  }

}

