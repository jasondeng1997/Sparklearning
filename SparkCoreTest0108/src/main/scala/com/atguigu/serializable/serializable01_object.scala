package com.atguigu.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object serializable01_object {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val u1 = new User
    u1.name = "zhangsan"
    val u2 = new User
    u2.name = "lisi"

    val u3 = new User
    u3.name = "atguigu"

    //因为咱们在executor端用到了driver端的对象,因此需要User类支持序列化,否则报错
//    val rdd = sc.makeRDD(List(u1,u2))

    val rdd:RDD[User] = sc.makeRDD(List())


    //因为代码里面出现了闭包环境,所以即使代码不执行,也会报错
    rdd.foreach(user => println(user.name + "love" + u3.name))

    //TODO 3 关闭资源
    sc.stop()

  }

}

/**
 * 序列化方式
 * 1.手动继承 extends Serializable
 * 2.直接使用样例类
 */
class User() {
  var name:String =_
}
