package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author layne
 */
object accumulator03_define {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //3. 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark", "Hive", "Hive"), 2)

    //需求:自定义累加器，统计RDD中首字母为“H”的单词以及出现的次数
    //1 创建累加器
    val myAcc = new MyAccumulator

    //2 注册累加器
    sc.register(myAcc,"acc")

    //3 使用累加器
    rdd.foreach(
      word => {
        myAcc.add(word)
      }
    )

    //4 获取累加器
    val accMap: mutable.Map[String, Long] = myAcc.value

    println(accMap)


    //TODO 3 关闭资源
    sc.stop()

  }

}

/**
 * 自定义累加器的步骤
 * 1.自定义一个类 继承 AccumulatorV2
 * 2.定义输入输出泛型
 * 输入:  Hello    => String
 * 输出: (Hello,4) (Hive,2)  => mutable.Map[String, Long]
 * 3. 重写6个抽象方法
 */

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

  //0 先定义一个可变的Map用来返回
  var map: mutable.Map[String, Long] = mutable.Map[String, Long]()

  //1 判断累加器是否初始化方法
  override def isZero: Boolean = map.isEmpty

  //2 复制累加器的方法
  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator

  //3 重置累加器的方法
  override def reset(): Unit = map.clear()

  //4 累加器在分区内(单个executor) 聚合的方法
  override def add(word: String): Unit = {
    if(word.startsWith("H")){
      map(word) = map.getOrElse(word,0L) + 1
    }
  }

  //5 累加器在分区间,driver端对多个累加器进行聚合的方法
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    //1 先获取到传入的累加器的值
    val map2: mutable.Map[String, Long] = other.value
    //2 遍历map2的key-value值,然后将map2和map相同的key的值,进行累加,然后覆盖掉map的key

    map2.foreach{
      case (word,cnt) => {
        map(word) = map.getOrElse(word,0L) + cnt
      }
    }

  }

  //6 返回累加器的值
  override def value: mutable.Map[String, Long] = map
}
