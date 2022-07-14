package com.atguigu.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object serializable02_function {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("hello")

//    val rdd2: RDD[String] = search.getMatch1(rdd)
    val rdd2: RDD[String] = search.getMatche2(rdd)

    rdd2.collect().foreach(println)




    //TODO 3 关闭资源
    sc.stop()

  }

}

/**
 * 结论:在executor用到了driver端的对象,对象的属性,对象的方法,spark都要求自定义的类支持序列化
 * 请你在spark开发的时候,如果需要自定义类型了,请务必序列化,推荐使用样例类
 */
class Search(query:String) {

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 函数序列化案例
  def getMatch1 (rdd: RDD[String]): RDD[String] = {

    rdd.filter(this.isMatch)
  }

  // 属性序列化案例
  def getMatche2(rdd: RDD[String]): RDD[String] = {
//    val str:String = this.query
    rdd.filter(x => x.contains(this.query))
//    rdd.filter(x => x.contains(str))

  }
}
