package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author layne
 */
object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(4))

    //3.创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    // oneAtATime = true 默认，一次读取队列里面的一个数据
    // oneAtATime = false， 按照设定的批次时间，读取队列里面数据
    val rddDStream: InputDStream[Int] = ssc.queueStream(rddQueue,false)


    val resDStream: DStream[Int] = rddDStream.reduce(_ + _)


    resDStream.print()



    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()

    //循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(1000)
    }


    ssc.awaitTermination()


  }

}
