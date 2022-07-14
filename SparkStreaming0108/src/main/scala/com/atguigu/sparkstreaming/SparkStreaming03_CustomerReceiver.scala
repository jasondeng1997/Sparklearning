package com.atguigu.sparkstreaming

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author layne
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    val inputDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

    val resDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    resDStream.print()



    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

}

class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  // receiver刚启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    //在onStart方法里面创建一个线程,专门用来接收数据
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }


  def receive(): Unit = {
    val socket = new Socket(host, port)

    val inputStream: InputStream = socket.getInputStream

    val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))

    //读取一行数据
    var input = reader.readLine()

    //当receiver没有关闭并且输入数据不为空，就循环发送数据给Spark
    while (input != null && !isStopped()){
      //读到一行数据以后,要存储到spark里
      store(input)

      input = reader.readLine()
    }

    // 如果循环结束，则关闭资源
    reader.close()
    socket.close()

    //重启接收任务
    restart("restart")
  }

  //receiver关闭之前调用的方法 一般不用实现
  override def onStop(): Unit = {

  }

}
