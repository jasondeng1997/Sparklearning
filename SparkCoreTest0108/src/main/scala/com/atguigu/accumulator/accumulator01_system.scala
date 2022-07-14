package com.atguigu.accumulator

import java.lang

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object accumulator01_system {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)),4)
    //需求:统计a出现的所有次数 ("a",10)
    //普通算子实现
    val rdd: RDD[(String, Int)] = dataRDD.reduceByKey(_ + _)
//    rdd.collect().foreach(println)

    //普通变量实现
    //普通变量无法实现 因为普通变量只能从driver端传到executor端,而不能再传回来
/*  var sum:Int = 0

    dataRDD.foreach{
      case (a,cnt) => {
        sum += cnt
        println("sum = "+sum)
      }
    }

    println(sum)
*/


    //累加器实现
    //1.创建累加器
    val acc: LongAccumulator = sc.longAccumulator("acc")

    //2.使用累加器
    dataRDD.foreach{
      case (a,cnt)=>{
        acc.add(cnt)
        //在executor端读取累加器的值,不是累加器最终的正确结果,因此咱们说累加器是分布式共享只写变量
        println(acc.value)
      }
    }

    //3.获取累加器
    val accSum: lang.Long = acc.value
    println("accSum = " + accSum)



    //TODO 3 关闭资源
    sc.stop()

  }

}
