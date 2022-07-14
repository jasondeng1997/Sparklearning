package com.atguigu.sparkstreaming

import org.apache.spark
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author layne
 */
object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))


    //Driver端,并且全局执行一次
    println("111111111:" + Thread.currentThread().getName)

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val resDStream: DStream[(String, Int)] = lineDStream.transform(
      rdd => {

        //Driver端,每个批次执行一次
        println("2222222222:" + Thread.currentThread().getName)

        val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val word2oneRDD: RDD[(String, Int)] = wordRDD.map(word => {

          //executor端 每个单词执行一次
          println("33333333:" + Thread.currentThread().getName)
          (word, 1)
        })
        val resRDD: RDD[(String, Int)] = word2oneRDD.reduceByKey(_ + _)
        resRDD
      }
    )


    resDStream.print()



    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

}
