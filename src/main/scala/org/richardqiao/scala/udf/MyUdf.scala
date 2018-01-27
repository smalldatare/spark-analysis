package org.richardqiao.scala.udf

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MyUdf {
  val concat_long_string = "concat_long_string"
  val group_concat_distinct = "group_concat_distinct"

  def concatLongStringUDF = (v1: Long, v2: String, split: String) => {
    v1 + split + v2
  }

  def getJsonObject = (json: String, field: String) => {
  }
  def main(args: Array[String]): Unit = {
    val data = Array((1, "Hadoop"), (2, "Spark"), (3, "Andy"), (4, "Apple"), (4, "Banana"), (4, "Apple"), (4, "Mongo"), (4, "Candy"))
    val conf = new SparkConf().setMaster("local").setAppName("aa")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    case class Test(v1: String, v2: String)
    import sqlContext.implicits._
    sqlContext.udf.register(concat_long_string, concatLongStringUDF)
    val rdd = sc.parallelize(data, 2)
    val df = rdd.toDF
    println("parLength: " + rdd.partitions.length)
    df.registerTempTable("test")
    sqlContext.sql("select _2,concat_long_string(3,_2,':') from test").collect.foreach(println)
    sc.stop
  }
}