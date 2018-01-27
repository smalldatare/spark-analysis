package org.richardqiao.scala.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.richardqiao.scala.spark.session.SessionAggrStatAccumulator
import org.richardqiao.java.constant.Constants

object SessionAggrStatAccumulatorTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CusterAccu").setMaster("local")
    val sc = new SparkContext(conf)
    val sessionAggrStatAccumulator = sc.accumulator("")(SessionAggrStatAccumulator)
    val rdd = sc.parallelize(Array(Constants.TIME_PERIOD_1s_3s, Constants.TIME_PERIOD_4s_6s, Constants.TIME_PERIOD_4s_6s, Constants.TIME_PERIOD_4s_6s), 2)
    rdd.foreach { sessionAggrStatAccumulator.add(_) }
    print("-------:" + sessionAggrStatAccumulator)

    val a = sc.accumulator(0)
    val parRdd = sc.parallelize(1 to 100, 3)
    parRdd.foreach { x => a.add(2) }
    println("a.value: " + a.value)

  }
}