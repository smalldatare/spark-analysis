package org.richardqiao.scala.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.richardqiao.java.util.SparkUtils

object MackDataTest {
  def main(args: Array[String]): Unit = {
    // 准备模拟数据
    val conf = new SparkConf().setAppName("CusterAccu").setMaster("local")
    SparkUtils.setMaster(conf);
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    SparkUtils.mockData(sc, sqlContext, true);
  }
}