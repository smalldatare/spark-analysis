package org.richardqiao.scala.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SqlTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CusterAccu").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    val df = sqlContext.read.json("examples/src/main/resources/people.json")
    // Displays the content of the DataFrame to stdout
    df.show()
  }
}