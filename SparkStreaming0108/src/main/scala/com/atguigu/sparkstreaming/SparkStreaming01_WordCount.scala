package com.atguigu.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author layne
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    val resDStream: DStream[(String, Int)] = word2oneDStream.reduceByKey(_ + _)

    resDStream.print()


    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()

  }

}
