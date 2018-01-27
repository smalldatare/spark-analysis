package org.richardqiao.scala.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd._

object FileTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("aa")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val path = "d:\\nio.txt"
    val rdd = sc.textFile(path)
    //    rdd.collect.foreach(println)
    rdd.toDF.write.json("d:\\filetest\\json")
  }
}