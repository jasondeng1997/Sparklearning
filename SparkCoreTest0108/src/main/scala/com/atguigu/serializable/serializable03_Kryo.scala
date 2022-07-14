package com.atguigu.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object serializable03_Kryo {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
      // 替换默认的序列化机制
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用kryo序列化的自定义类
      .registerKryoClasses(Array(classOf[Searche]))

    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val searche = new Searche("hello")
    val resRDD: RDD[String] = searche.getMatchedRDD1(rdd)

    resRDD.collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}

/**
 * 结论: 使用Kryo序列化步骤
 * 1.自定义类先实现java序列化机制
 * 2.在spark的conf配置文件中修改spark.serializer = org.apache.spark.serializer.KryoSerializer
 * 3.注册一下自定义的类 实现kryo  registerKryoClasses(Array(classOf[Searche]))
 */
case class Searche(val query: String){

  def isMatch(s: String) = {
    s.contains(query)
  }

  def getMatchedRDD1(rdd: RDD[String]) = {
    rdd.filter(isMatch)
  }

  def getMatchedRDD2(rdd: RDD[String]) = {
    rdd.filter(_.contains(this.query))
  }
}

