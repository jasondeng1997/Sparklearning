package com.atguigu.accumulator

import java.lang

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object accumulator02_updateCount {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)),4)
    //需求:统计a出现的所有次数 ("a",10)

    //累加器实现
    //1.创建累加器
    val acc: LongAccumulator = sc.longAccumulator("acc")

    //2.使用累加器
    val mapRDD: RDD[Unit] = dataRDD.map {
      case (a, cnt) => {
        acc.add(cnt)
        //在executor端读取累加器的值,不是累加器最终的正确结果,因此咱们说累加器是分布式共享只写变量
        println(acc.value)
      }
    }

    mapRDD.collect()
    mapRDD.collect()



    //3.获取累加器
    val accSum: lang.Long = acc.value
    println("accSum = " + accSum)



    //TODO 3 关闭资源
    sc.stop()

  }

}
