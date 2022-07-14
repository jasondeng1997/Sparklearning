package com.atguigu.accumulator

import scala.collection.mutable

/**
 * @author layne
 */
object MapTest {
  def main(args: Array[String]): Unit = {
    var map: mutable.Map[String, Long] = mutable.Map[String, Long]()

    println(map)

    map("Hello") = map.getOrElse("Hello",0L) + 1
    map("Hello") = map.getOrElse("Hello",0L) + 1
    map("Hello") = map.getOrElse("Hello",0L) + 1
    map("Hello") = 100

    println(map)
  }

}
