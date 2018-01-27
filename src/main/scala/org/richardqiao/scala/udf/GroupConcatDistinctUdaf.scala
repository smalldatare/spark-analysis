package org.richardqiao.scala.udf

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class GroupConcatDistinctUdaf extends UserDefinedAggregateFunction {
  val seperator = ","
  //指定输入数据的字段与类型
  override def inputSchema: StructType = StructType(Array(StructField("name", StringType, true)))

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val v2 = input.getString(0)
    val v1 = buffer.getString(0)
    println("update  v1:" + v1 + ", v2:" + v2)
    if (v1.equals("")) {
      buffer(0) = v2
    } else if (!v1.contains(v2)) buffer(0) = v1 + seperator + v2
  }
  //指定缓冲数据的字段类型
  override def bufferSchema: StructType = StructType(Array(StructField("group", StringType, true)))

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val v2 = buffer2.getString(0)
    var v1 = buffer1.getString(0)
    println("merge:  v1:" + v1 + ", v2:" + v2)

    v1 match {
      case "" => buffer1(0) = v2
      case _ => {

        val bufferSplited1 = v1.split(seperator)
        val bufferSplited2 = v2.split(seperator)
        for (s <- bufferSplited2) {
          if (!v1.contains(s)) {
            v1 = v1 + seperator + s
          }
        }
        buffer1(0) = v1
      }
    }
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
    //        buffer.update(0, "")  
  }
  //指定是否是确定性的
  override def deterministic: Boolean = true
  //
  override def evaluate(buffer: Row): Any = buffer.getString(0)
  //指定返回的类型
  override def dataType: DataType = StringType
}

object GroupConcatDistinctUdaf {
  def main(args: Array[String]): Unit = {
    val data = Array((1, "Hadoop"), (2, "Spark"), (3, "Andy"), (4, "Apple"), (4, "Banana"), (4, "Apple"), (4, "Mongo"), (4, "Candy"))
    val conf = new SparkConf().setMaster("local").setAppName("aa")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    case class Test(v1: String, v2: String)
    import sqlContext.implicits._
    sqlContext.udf.register("gc", new GroupConcatDistinctUdaf)
    val rdd = sc.parallelize(data, 2)
    val df = rdd.toDF
    println("parLength: " + rdd.partitions.length)
    df.registerTempTable("test")
    sqlContext.sql("select _1,gc(_2) from test group by _1").collect.foreach(println)
  }
}