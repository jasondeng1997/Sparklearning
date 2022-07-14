package com.atguigu.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @author layne
 */
object KeyValue01_partitionBy {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)

    val rdd2: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    rdd2.mapPartitionsWithIndex(
          (index,iters)=>iters.map((index,_))
        ).collect().foreach(println)


    //TODO 3 关闭资源
    sc.stop()

  }

}

class MyPartitioner(num :Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      val ketInt: Int = key.asInstanceOf[Int]
      if(ketInt % 2 == 0){
        0
      }else 1

    }else{
      1
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: MyPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

}

