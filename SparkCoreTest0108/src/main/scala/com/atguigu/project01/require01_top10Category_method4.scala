package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author layne
 */
object require01_top10Category_method4 {
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

    //3 使用累加器解决问题
    //3.1 创建累加器
    val acc = new CategoryCountAccumulator

    //3.2 注册累加器
    sc.register(acc,"acc")

    //3.3 使用累加器
    actionRDD.foreach(
      action => acc.add(action)
    )

    //3.4 获取累加器
    val accMap: mutable.Map[(String, String), Long] = acc.value

    //4 按照品类id分组
    val groupMap: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)

    //5 转换数据结构
    val infoIter: immutable.Iterable[CategoryCountInfo] = groupMap.map {
      case (id, map) => {
        val click = map.getOrElse((id, "click"), 0L)
        val order = map.getOrElse((id, "order"), 0L)
        val pay = map.getOrElse((id, "pay"), 0L)

        CategoryCountInfo(id, click, order, pay)
      }
    }

    //6 倒序排序取前10
    infoIter.toList.sortBy(info => (info.clickCount,info.orderCount,info.payCount))(Ordering[(Long,Long,Long)].reverse).take(10).foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}

/**
 * 自定义累加器
 * 输入 UserVisitAction
 * 输出 mutable.Map[(String,String),Long]
 *
 * 实现6个抽象方法
 */
class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{
  var map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new CategoryCountAccumulator

  override def reset(): Unit = map.clear()

  override def add(action: UserVisitAction): Unit = {
    if (action.click_category_id != "-1") {
      //点击action
      val key = (action.click_category_id,"click")
      map(key) = map.getOrElse(key,0L) + 1
    }else if(action.order_category_ids != "null"){
      //下单action
      val ids: Array[String] = action.order_category_ids.split(",")

      for (id <- ids) {
        val key = (id,"order")
        map(key) = map.getOrElse(key,0L) + 1
      }

    }else if(action.pay_category_ids != "null"){
      //支付action
      val ids: Array[String] = action.pay_category_ids.split(",")

      for (id <- ids) {
        val key = (id,"pay")
        map(key) = map.getOrElse(key,0L) + 1
      }

    }

  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    val map2: mutable.Map[(String, String), Long] = other.value
    map2.foreach{
      case (key,cnt) => {
        map(key) = map.getOrElse(key,0L) + cnt
      }
    }

  }

  override def value: mutable.Map[(String, String), Long] = map
}
