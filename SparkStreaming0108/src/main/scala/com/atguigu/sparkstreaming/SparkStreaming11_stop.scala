package com.atguigu.sparkstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author layne
 */
object SparkStreaming11_stop {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //开启spark的优雅关闭参数
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")

    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    val resDStream: DStream[(String, Int)] = word2oneDStream.reduceByKey(_ + _)

    resDStream.print()


    //额外启动一个线程,用来关闭ssc
    new Thread(new MonitorStop(ssc)).start()

    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()

    ssc.awaitTermination()

  }

}

class MonitorStop(ssc:StreamingContext) extends Runnable{
  override def run(): Unit = {

    //在线程类里面找一个外部的控制条件(开关)
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"),new Configuration(),"atguigu")

    while (true) {

      Thread.sleep(5000)

      val result: Boolean = fs.exists(new Path("hdfs://hadoop102:8020/stopSpark"))

      if(result){

        val state: StreamingContextState = ssc.getState()

        if(state == StreamingContextState.ACTIVE){
          ssc.stop(true,true)
          System.exit(0)
        }
      }
    }
  }
}
