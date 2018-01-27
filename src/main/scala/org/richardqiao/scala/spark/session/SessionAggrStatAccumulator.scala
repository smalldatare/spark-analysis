package org.richardqiao.scala.spark.session

import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.richardqiao.java.util._
import org.richardqiao.java.constant._
object SessionAggrStatAccumulator extends AccumulatorParam[String] {
  //如果初始值为空，返回v2  
  def addInPlace(v1: String, v2: String): String = {
    if (v1 == "") {
      println("----v1=''" + ",v2:" + v2)
      v2
    } else {
      if (v2.length() > 20) {

        val s1 = v1.split("\\|")
        val s2 = v2.split("\\|")
        val ab = new StringBuffer();
        var v3: String = zero("");
        for (i <- 0 until s1.length) {
          val newVal = s1(i).split("=")(1).toLong + s2(i).split("=")(1).toLong + ""
          v3 = StringUtils.setFieldInConcatString(v3, "\\|", s1(i).split("=")(0), Integer.valueOf(newVal) + "")
        }
        println("v2.length()》=20 ：   v1:" + v1 + ",  v2:" + v2 + "  ,v3:" + v3)
        v3
      } else {
        val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)

        val v3 = StringUtils.setFieldInConcatString(v1, "\\|", v2, Integer.valueOf(oldValue) + 1 + "")
        println("v1:" + v1 + ",  v2:" + v2 + " ,v3=" + v3)
        v3
      }
    }
  }
  def zero(initialValue: String): String = Constants.SESSION_COUNT + "=0|" +
    Constants.TIME_PERIOD_1s_3s + "=0|" +
    Constants.TIME_PERIOD_4s_6s + "=0|" +
    Constants.TIME_PERIOD_7s_9s + "=0|" +
    Constants.TIME_PERIOD_10s_30s + "=0|" +
    Constants.TIME_PERIOD_30s_60s + "=0|" +
    Constants.TIME_PERIOD_1m_3m + "=0|" +
    Constants.TIME_PERIOD_3m_10m + "=0|" +
    Constants.TIME_PERIOD_10m_30m + "=0|" +
    Constants.TIME_PERIOD_30m + "=0|" +
    Constants.STEP_PERIOD_1_3 + "=0|" +
    Constants.STEP_PERIOD_4_6 + "=0|" +
    Constants.STEP_PERIOD_7_9 + "=0|" +
    Constants.STEP_PERIOD_10_30 + "=0|" +
    Constants.STEP_PERIOD_30_60 + "=0|" +
    Constants.STEP_PERIOD_60 + "=0";

  //    def main(args: Array[String]): Unit = {
  //        
  //        val conf  = new SparkConf().setAppName("CusterAccu").setMaster("local")
  //            val sc = new SparkContext(conf)
  //        val sessionAggrStatAccumulator  = sc.accumulator("")(SessionAggrStatAccumulator)
  //        val rdd =  sc.parallelize(Array(Constants.TIME_PERIOD_1s_3s,Constants.TIME_PERIOD_4s_6s,Constants.TIME_PERIOD_4s_6s,Constants.TIME_PERIOD_4s_6s), 2)
  //        rdd.foreach { sessionAggrStatAccumulator.add(_) } 
  //        print("-------:"+sessionAggrStatAccumulator)
  //    }

}