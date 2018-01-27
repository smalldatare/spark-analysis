package org.richardqiao.scala.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object KryoTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")
    //              .registerKryoClasses() 
    val sc = new SparkContext(conf)

  }

}