package com.atguigu.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object require01_top10Category_method2 {
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
    val infoRDD: RDD[CategoryCountInfo] = actionRDD.flatMap(
      action => {
        if (action.click_category_id != "-1") {
          //点击的数据
          List(CategoryCountInfo(action.click_category_id, 1, 0, 0))
        } else if (action.order_category_ids != "null") {
          //下单的数据
          val ids: Array[String] = action.order_category_ids.split(",")

          ids.map(id => CategoryCountInfo(id, 0, 1, 0))

        } else if (action.pay_category_ids != "null") {
          //支付的数据
          val ids: Array[String] = action.pay_category_ids.split(",")

          ids.map(id => CategoryCountInfo(id, 0, 0, 1))

        } else {
          Nil
        }
      }
    )

    //4 按照品类id分组 形成key value结构
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(info => info.categoryId)

    //5 聚合每个品类分组下info数据
    val reduceRDD: RDD[CategoryCountInfo] = groupRDD.mapValues(
      iter => {
        iter.reduce(
          (info1, info2) => {
            info1.clickCount += info2.clickCount
            info1.orderCount += info2.orderCount
            info1.payCount += info2.payCount
            info1
          }
        )
      }
    ).map(_._2)

    //6 倒序排序取前10
    reduceRDD.sortBy(info => (info.clickCount,info.orderCount,info.payCount),false).take(10).foreach(println)

    //TODO 3 关闭资源
    sc.stop()

  }

}

//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: String,//用户的ID
                           session_id: String,//Session的ID
                           page_id: String,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: String,//某一个商品品类的ID
                           click_product_id: String,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: String)//城市 id
// 输出结果表
case class CategoryCountInfo(categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数

