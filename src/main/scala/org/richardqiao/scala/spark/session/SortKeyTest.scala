package org.richardqiao.scala.spark.session

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortKeyTest {
  def main(args: Array[String]): Unit = {
    println(33)
    val conf = new SparkConf().setMaster("local").setAppName("a")
    val sc = new SparkContext(conf)
    val s1 = SortKey(10, 9, 3)
    val s2 = SortKey(3, 2, 1)
    val list = List((SortKey(30, 35, 40), 1), (SortKey(35, 30, 40), 2), (SortKey(30, 38, 30), 3), (SortKey(30, 38, 32), 4))
    val rdd = sc.parallelize(list)
    rdd.sortByKey(false).collect.foreach(println)
    sc.stop
  }
}