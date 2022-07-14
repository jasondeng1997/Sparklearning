package com.atguigu.sparkstreaming

import org.apache.spark
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author layne
 */
object SparkStreaming10_output {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))


    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    val resDStream: DStream[(String, Int)] = word2oneDStream.reduceByKey(_ + _)

    println("111111:" + Thread.currentThread().getName)

    resDStream.foreachRDD(
      rdd => {
        println("222222:" + Thread.currentThread().getName)

        rdd.foreachPartition(
          //创建数据库连接
          iter => {

            println("33333333:" + Thread.currentThread().getName)

            //使用数据库连接
            iter.foreach(
              println
            )
          }
        )
      }
    )


    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

}
