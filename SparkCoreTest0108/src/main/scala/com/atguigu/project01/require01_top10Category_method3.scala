package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object require01_top10Category_method3 {
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
    reduceRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10).foreach(println)

    //TODO 3 关闭资源
    sc.stop()

  }

}

