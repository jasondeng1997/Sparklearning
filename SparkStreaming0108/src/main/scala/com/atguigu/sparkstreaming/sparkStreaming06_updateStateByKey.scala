package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author layne
 */
object sparkStreaming06_updateStateByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint("D:\\IdeaProjects\\SparkStreaming0108\\ck")

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    val resDStream: DStream[(String, Int)] = word2oneDStream.updateStateByKey(updateFunc)

    resDStream.print()





    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc = (seq:Seq[Int], state:Option[Int]) => {
    //获取当前批次的单词和
    val currentSum: Int = seq.sum

    //获取状态数据的单词和
    val stateSum: Int = state.getOrElse(0)

    //返回当前批次的单词和 + 历史状态的单词和
    Some(currentSum + stateSum)

  }

}
