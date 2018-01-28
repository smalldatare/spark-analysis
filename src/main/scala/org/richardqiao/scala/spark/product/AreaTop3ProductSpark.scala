package org.richardqiao.scala.spark.product

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.richardqiao.java.util.SparkUtils
import org.richardqiao.java.dao.factory.DAOFactory
import org.richardqiao.java.util.ParamUtils
import org.richardqiao.java.constant.Constants
import java.util.Properties
import org.richardqiao.java.conf.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.richardqiao.scala.udf.MyUdf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.richardqiao.scala.udf.GroupConcatDistinctUdaf

object AreaTop3ProductSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CusterAccu").setMaster("local")
    SparkUtils.setMaster(conf);
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val productInfo = sqlContext.read.format("json").load("/Users/lqiao/code/spark-analysis/src/main/resources/testdata/product_info")
    val user_info = sqlContext.read.format("json").load("/Users/lqiao/code/spark-analysis/src/main/resources/testdata/user_info")
    val user_visit_action = sqlContext.read.format("json").load("/Users/lqiao/code/spark-analysis/src/main/resources/testdata/user_visit_action")

    user_visit_action.take(3).foreach(println)
    user_visit_action.registerTempTable("user_visit_action")
    val startDate = "2016-04-01"
    val endDate = "2016-04-30"
    val cityid2ClickActionRDD = getcityid2ClickActionRDDByDate(sqlContext, startDate, endDate)
    cityid2ClickActionRDD.take(3).foreach(println)
    //   if(1==1) return
    val cityId2CityInfoRdd = getcityid2CityInfoRDD(sqlContext)
    //      if(1==1) return
    import MyUdf._
    sqlContext.udf.register(concat_long_string, concatLongStringUDF)
    sqlContext.udf.register(group_concat_distinct, new GroupConcatDistinctUdaf)
    // 生成点击商品基础信息临时表
    // 技术点3：将RDD转换为DataFrame，并注册临时表
    val generateTempClickProductBasic = generateTempClickProductBasicTable(sqlContext, cityid2ClickActionRDD, cityId2CityInfoRdd)
    generateTempClickProductBasic.show

    // 生成各区域各商品点击次数的临时表
    val tempAreaPrdocutClickCountDF = generateTempAreaPrdocutClickCountTable(sqlContext);
    tempAreaPrdocutClickCountDF.show
  }

  def getcityid2ClickActionRDDByDate(sqlContext: SQLContext, startDate: String, endDate: String) = {
    // 从user_visit_action中，查询用户访问行为数据
    // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
    // 第二个限定：在用户指定的日期范围内的数据

    val sql =
      "SELECT " + "city_id," + "click_product_id product_id " + "FROM user_visit_action " + "WHERE click_product_id IS NOT NULL " + "AND date>='" + startDate + "' " + "AND date<='" + endDate + "'";
    val cityid2ClickActionRDD = sqlContext.sql(sql).map { row =>
      val cityId = row.getAs[Long]("city_id")
      (cityId, row)
    }
    cityid2ClickActionRDD
  }
  def getcityid2CityInfoRDD(sqlContext: SQLContext) = {
    // 构建MySQL连接配置信息（直接从配置文件中获取）
    var url: String = null;
    var user: String = null;
    var password: String = null;
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

    if (local) {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL);
      user = ConfigurationManager.getProperty(Constants.JDBC_USER);
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
    } else {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
      user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
    }
    val urlWithPwd = url + "?user=" + user + "&password=" + password
    println(urlWithPwd)
    val cityInfoDf = sqlContext.read.format("jdbc")
      .option("url", urlWithPwd)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "city_info")
      .option("user", "root")
      .option("password", "")
      .load()
//    val cityInfoDf = sqlContext.read.jdbc(urlWithPwd, "city_info", new Properties())
    cityInfoDf.printSchema()
    val cityId2CityInfo = cityInfoDf.map(row => (row.getInt(0).toLong, row))
    //    cityInfoDf.map(row=>(row.getAs("city_id"),row))
    cityInfoDf.show
    cityId2CityInfo
  }
  def generateTempClickProductBasicTable(sqlContext: SQLContext, cityid2ClickActionRDD: RDD[(Long, Row)], cityId2CityInfo: RDD[(Long, Row)]) = {
    val rdd = cityid2ClickActionRDD.join(cityId2CityInfo)

    val rowRDD = rdd.map { f: (Long, (Row, Row)) =>
      {
        val cityId = f._1
        val clickActionRow = f._2._1
        val cityInfoRow = f._2._2
        val productId = clickActionRow.getAs[Long]("product_id")
        val cityName = cityInfoRow.getAs[String](1)
        val area = cityInfoRow.getAs[String](2)
        Row(cityId, cityName, area, productId)
      }
    }
    val schema = StructType(Array(StructField("city_id", LongType, true),
      StructField("city_name", StringType, true),
      StructField("area", StringType, true),
      StructField("product_id", LongType, true)))

    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.registerTempTable("tmp_click_product_basic")
    df
  }
  def generateTempAreaPrdocutClickCountTable(sqlContext: SQLContext) = {
    val sql =
      "select area,product_id," +
        "count(*) click_count," +
        "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos from tmp_click_product_basic group by area,product_id"

    //   val sql = 
    //        "SELECT "+
    //           "area,"+
    //          "product_id,"+
    //           "count(*) click_count, "  +
    //          "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "  +
    //         "FROM tmp_click_product_basic "+
    //         "GROUP BY area,product_id ";
    val df = sqlContext.sql(sql)
    df
  }
  def generateTempAreaFullProductClickCountTable(sqlContext: SQLContext) = {
    val sql =
      "SELECT " +
        "tapcc.area,"
    "tapcc.product_id," +
      "tapcc.click_count," +
      "tapcc.city_infos," +
      "pi.product_name," +
      "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
      "FROM tmp_area_product_click_count tapcc " +
      "JOIN product_info pi ON tapcc.product_id=pi.product_id ";
    val df = sqlContext.sql(sql)
    df.registerTempTable("tmp_area_fullprod_click_count");
  }
}