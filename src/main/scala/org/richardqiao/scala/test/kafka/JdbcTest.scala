package org.richardqiao.scala.test.kafka

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object JdbcTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("aa")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    //    val jdbcDF = sqlContext.read.format("jdbc").options(
    ////        Map("url" -> "jdbc:postgresql:dbserver",
    //             Map("url" -> "jdbc:mysql//localhost:3306/sparkproject?user=root&password=123456",
    //        "dbtable" -> "task")).load()
    //jdbcDF.show

    val reader = sqlContext.read.format("jdbc")
    reader.option("url", "jdbc:mysql//192.168.14.1:3306/sparkproject")
    reader.option("dbtable", "task")
    reader.option("driver", "com.mysql.jdbc.Driver")
    reader.option("user", "root")
    reader.option("password", "123456")
    val df = reader.load()
    df.show

  }
}