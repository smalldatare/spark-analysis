package org.richardqiao.scala.spark.session

import java.lang.{ Long => Jlong }
import java.util.ArrayList
import java.util.Date
import java.util.HashMap
import java.util.HashSet
import java.util.Random
import scala.collection.JavaConverters._
import org.apache.spark.Accumulator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.richardqiao.java.constant.Constants
import org.richardqiao.java.dao.factory.DAOFactory
import org.richardqiao.java.domain.SessionAggrStat
import org.richardqiao.java.domain.Top10Category
import org.richardqiao.java.util._
import org.richardqiao.java.util.DateUtils
import org.richardqiao.java.util.ParamUtils
import org.richardqiao.java.domain.Top10Session
/*
 * List(List(1,2),List(3,4)).flatMap { x => x.map { _+1} }
 * List(List(1,2),List(3,4)).map { x => x.map {y=>(y,y)} }.flatten
 * List(List(1,2),List(3,4)).flatMap { x => x.map {y=>(y,y)} }
 */
object UserVisitSessionAnalyzeSpark {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CusterAccu").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    val productInfo = sqlContext.read.format("json").load("/Users/lqiao/code/spark-analysis/src/main/resources/testdata/product_info")
    val user_info = sqlContext.read.format("json").load("/Users/lqiao/code/spark-analysis/src/main/resources/testdata/user_info")
    val user_visit_action = sqlContext.read.format("json").load("/Users/lqiao/code/spark-analysis/src/main/resources/testdata/user_visit_action")
    //productInfo.take(1).foreach { println }

    println("user---")
    val sessionid2actionRDD = user_visit_action.map { x => (x.getString(x.fieldIndex("session_id")), x) }
    sessionid2actionRDD.persist
    val userInfoDf = user_info.map { x => (x.getLong(x.fieldIndex("user_id")), x) }

    // 创建需要使用的DAO组件
    val taskDAO = DAOFactory.getTaskDAO();

    // 首先得查询出来指定的任务，并获取任务的查询参数
    val taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
    val task = taskDAO.findById(taskid);
    println("taskId:" + task)

    if (task == null) {
      System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
      return ;
    }
    //    val taskParam =  GooJsonObject(task.getTaskParam());
    //    val a = new AliJSONObject()
    //    AliJSONObject.
    //    val rdd = sc.parallelize(Array(("a",1),("b",2),("a",3)))
    //   val t = rdd.groupBy(_._1)
    //    rdd.groupByKey().collect;
    val sessionid2ActionsRDD = sessionid2actionRDD.groupByKey
    sessionid2ActionsRDD.persist
    val userIdParAggr = sessionid2ActionsRDD.map { tuple =>
      val searchKeywordsBuffer = new HashSet[String]()
      val clickCategoryIdsBuffer = new HashSet[Long]()
      var userid: Long = 0
      var startTime: Date = null;
      var endTime: Date = null;
      // session的访问步长
      var stepLength = 0;
      val iter = tuple._2
      val sessionId = tuple._1
      for (row <- iter) {
        if (userid == 0) userid = row.getAs("user_id")
        Option(row.getAs[Long]("click_category_id")).foreach { clickCategoryIdsBuffer add }
        Option(row.getAs[String]("search_keyword")).map { searchKeywordsBuffer add }
        // 计算session开始和结束时间
        val actionTime = DateUtils.parseTime(row.getAs[String]("action_time"));

        if (startTime == null) {
          startTime = actionTime;
        }
        if (endTime == null) {
          endTime = actionTime;
        }

        if (actionTime.before(startTime)) {
          startTime = actionTime;
        }
        if (actionTime.after(endTime)) {
          endTime = actionTime;
        }

        stepLength = stepLength + 1;
      }
      val searchKeywords = searchKeywordsBuffer.toArray().mkString(",")
      val clickCategoryIds = clickCategoryIdsBuffer.toArray().mkString(",")
      // 计算session访问时长（秒）
      val visitLength = (endTime.getTime - startTime.getTime) / 1000L

      // 聚合数据，用什么样的格式进行拼接？
      // 我们这里统一定义，使用key=value|key=value
      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
      (userid, partAggrInfo)
    }
    val userid2FullInfoRDD = userIdParAggr.join(userInfoDf)

    val sessionid2FullAggrInfoRDD = userid2FullInfoRDD.map { full =>
      {
        val partAggrInfo = full._2._1
        val userInfoRow = full._2._2

        val age = Option(userInfoRow.getAs[Int]("age"))
        val professional = Option(userInfoRow.getAs[String]("professional"))
        val city = Option(userInfoRow.getAs[String]("city"))
        val sex = Option(userInfoRow.getAs[Int]("sex"));

        val fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age.getOrElse("") + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional.getOrElse("") + "|" + Constants.FIELD_CITY + "=" + city.getOrElse("") + "|" + Constants.FIELD_SEX + "=" + sex.getOrElse("");

        val sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, Constants.FIELD_SESSION_ID);
        (sessionId, fullAggrInfo)
      }
    }
    //    println("fullcount:"+ sessionid2FullAggrInfoRDD.count())
    //   if(1==1) true;
    val accumulator = sc.accumulator("")(SessionAggrStatAccumulator)
    val filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2FullAggrInfoRDD, "", accumulator)

    filteredSessionid2AggrInfoRDD.count //实现随机抽取后，这个job取消

    //    randomExtractSession(sessionid2FullAggrInfoRDD)
    val accuValue = accumulator.value
    /**
     * 计算各session范围占比，并写入MySQL
     */
    calculateAndPersistAggrStat(accuValue, task.getTaskid.toString());

    println("accu: " + accumulator)

    val sessionId2DetailsRDD = getSessionId2DetailsRDD(filteredSessionid2AggrInfoRDD, sessionid2actionRDD)
    val top10Category = getTop10Category(taskid, sessionId2DetailsRDD)
    //保存到表
    saveTop10Category2DB(taskid, top10Category)

    sc.stop

  }

  def randomExtractSession(sessionid2FullAggrInfoRDD: RDD[(String, String)]) = {
    //第一步，计算每天每小时session数量
    val time2sessionidRDD = sessionid2FullAggrInfoRDD.map { x =>
      val startTime = StringUtils.getFieldFromConcatString(x._2, "\\|", Constants.FIELD_START_TIME)
      val dateHH = DateUtils.getDateHour(startTime)
      (dateHH, x._2)
    }
    /**
     * 每天每小时的session数量的计算
     * 是有可能出现数据倾斜的吧，这个是没有疑问的
     * 比如说大部分小时，一般访问量也就10万；但是，中午12点的时候，高峰期，一个小时1000万
     * 这个时候，就会发生数据倾斜789oui
     *
     * 我们就用这个countByKey操作，给大家演示第三种和第四种方案
     *
     */
    val dayHH2Count = time2sessionidRDD.countByKey

    /**
     * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
     */
    val day2HourCountMap = new HashMap[String, HashMap[String, Long]]();
    // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式

    for (k <- dayHH2Count.keys) {
      val strs = k.split("_")
      val day = strs(0)
      val hour = strs(1)
      var hourCountMap = day2HourCountMap.get(day);
      if (hourCountMap == null) {
        hourCountMap = new HashMap[String, Long]();
        day2HourCountMap.put(day, hourCountMap)
      }
      hourCountMap.put(hour, dayHH2Count.get(k).get)
    }
    val extractNumberPerDay = 100 / day2HourCountMap.size.toDouble
    val dateHourExtractMap =
      new HashMap[String, HashMap[String, List[Int]]]();
    val random = new Random();
  }

  /**
    * 计算各session范围占比，并写入MySQL
    * @param value
    * @param taskid
    */
  def calculateAndPersistAggrStat(value: String, taskid: String) = {
    // 从Accumulator统计串中获取值
    val session_count = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.SESSION_COUNT).toDouble;

    val visit_length_1s_3s = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_1s_3s).toLong;
    val visit_length_4s_6s = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_4s_6s).toLong;
    val visit_length_7s_9s = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_7s_9s).toLong;
    val visit_length_10s_30s = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_10s_30s).toLong;
    val visit_length_30s_60s = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_30s_60s).toLong;
    val visit_length_1m_3m = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_1m_3m).toLong;
    val visit_length_3m_10m = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_3m_10m).toLong;
    val visit_length_10m_30m = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_10m_30m).toLong;
    val visit_length_30m = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.TIME_PERIOD_30m).toLong;

    val step_length_1_3 = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.STEP_PERIOD_1_3).toLong;
    val step_length_4_6 = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.STEP_PERIOD_4_6).toLong;
    val step_length_7_9 = StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.STEP_PERIOD_7_9).toLong;
    val step_length_10_30 = Jlong.valueOf(StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.STEP_PERIOD_10_30));
    val step_length_30_60 = Jlong.valueOf(StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.STEP_PERIOD_30_60));
    val step_length_60 = Jlong.valueOf(StringUtils.getFieldFromConcatString(
      value, "\\|", Constants.STEP_PERIOD_60));

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(
      visit_length_1s_3s / session_count, 2);
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(
      visit_length_4s_6s / session_count, 2);
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(
      visit_length_7s_9s / session_count, 2);
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(
      visit_length_10s_30s / session_count, 2);
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(
      visit_length_30s_60s / session_count, 2);
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(
      visit_length_1m_3m / session_count, 2);
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(
      visit_length_3m_10m / session_count, 2);
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(
      visit_length_10m_30m / session_count, 2);
    val visit_length_30m_ratio = NumberUtils.formatDouble(
      visit_length_30m / session_count, 2);

    val step_length_1_3_ratio = NumberUtils.formatDouble(
      step_length_1_3 / session_count, 2);
    val step_length_4_6_ratio = NumberUtils.formatDouble(
      step_length_4_6 / session_count, 2);
    val step_length_7_9_ratio = NumberUtils.formatDouble(
      step_length_7_9 / session_count, 2);
    val step_length_10_30_ratio = NumberUtils.formatDouble(
      step_length_10_30 / session_count, 2);
    val step_length_30_60_ratio = NumberUtils.formatDouble(
      step_length_30_60 / session_count, 2);
    val step_length_60_ratio = NumberUtils.formatDouble(
      step_length_60 / session_count, 2);

    // 将统计结果封装为Domain对象
    val sessionAggrStat = new SessionAggrStat();
    sessionAggrStat.setTaskid(taskid.toLong);
    sessionAggrStat.setSession_count(session_count.toLong);
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

    // 调用对应的DAO插入统计结果
    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
    sessionAggrStatDAO.insert(sessionAggrStat);
  }

  def filterSessionAndAggrStat(sessionid2FullAggrInfoRDD: RDD[(String, String)], taskParam: String, sessionAggrStatAccumulator: Accumulator[String]) = {
    val startAge = 10;
    val endAge = 30
    val professionals = 3
    val cities = ""
    val sex = "m"
    val keywords = ""
    val categoryIds = ""

    val filtered = sessionid2FullAggrInfoRDD.filter { f =>
      val aggrInfo = f._2
      if (1 < 0) { //过滤出
        false;
      }

      // 主要走到这一步，那么就是需要计数的session
      sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

      // 计算出session的访问时长和访问步长的范围，并进行相应的累加

      // 计算出session的访问时长和访问步长的范围，并进行相应的累加
      val visitLength = StringUtils.getFieldFromConcatString(
        aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
      val stepLength = StringUtils.getFieldFromConcatString(
        aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
      calculateVisitLength(visitLength)
      calculateStepLength(stepLength)

      def calculateVisitLength(visitLength: Long) = {
        if (visitLength >= 0 && visitLength <= 3) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
        } else if (visitLength >= 4 && visitLength <= 6) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
        } else if (visitLength >= 7 && visitLength <= 9) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
        } else if (visitLength >= 10 && visitLength <= 30) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
        } else if (visitLength > 30 && visitLength <= 60) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
        } else if (visitLength > 60 && visitLength <= 180) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
        } else if (visitLength > 180 && visitLength <= 600) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
        } else if (visitLength > 600 && visitLength <= 1800) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
        } else if (visitLength > 1800) {
          sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
        }
      }
      def calculateStepLength(stepLength: Long) = {
        if (stepLength >= 1 && stepLength <= 3) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
        } else if (stepLength >= 4 && stepLength <= 6) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
        } else if (stepLength >= 7 && stepLength <= 9) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
        } else if (stepLength >= 10 && stepLength <= 30) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
        } else if (stepLength > 30 && stepLength <= 60) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
        } else if (stepLength > 60) {
          sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
        }
      }
      true
    }

    filtered
  }
  //    def getTop10Category(filteredSessionid2AggrInfoRDD : RDD[(String, String)], sessionid2ActionsRDD:RDD[(String, Iterable[Row])]) = {
  //    //获取符合条件的sesseion明细数据  //sessionId,Row
  //     val sessionId2DetailsRDD =  filteredSessionid2AggrInfoRDD.join(sessionid2ActionsRDD).map(x=>(x._1,x._2._2))
  //    //获取session访问过的所有品类id
  //     //访问过：指的是点击过，下单过，支付过的品类
  //    val visitedCategory = sessionId2DetailsRDD.map{x=>
  //       val rows = x._2
  //       var list = new ArrayList[Tuple2[Long,Long]]
  //       for(row <- rows.iterator){
  //         val click_category_id = row.getAs[Long]("click_category_id")
  //         val order_product_ids = Option(row.getAs[String]("order_product_ids"))
  //         val pay_category_ids = Option(row.getAs[String]("pay_category_ids"))
  //         list.add((click_category_id,click_category_id))
  //         order_product_ids.map { _.split(",") }.map(x=>x.foreach { s => list.add((s.toLong,s.toLong)) })
  //         pay_category_ids.map { _.split(",") }.map(x=>x.foreach { s => list.add((s.toLong,s.toLong)) })
  //    
  //       }
  //       list
  //     }
  ////     sessionid2actionRDD
  //  }

  def getSessionId2DetailsRDD(filteredSessionid2AggrInfoRDD: RDD[(String, String)], sessionid2ActionsRDD: RDD[(String, Row)]) = {
    filteredSessionid2AggrInfoRDD.join(sessionid2ActionsRDD).map(x => (x._1, x._2._2))
  }

  def getTop10Category(taskid: Long, sessionId2DetailsRDD: RDD[(String, Row)]) = {
    //  val sessionId2DetailsRDD =  filteredSessionid2AggrInfoRDD.join(sessionid2ActionsRDD).map(x=>(x._1,x._2._2))
    var categoryidRDD = sessionId2DetailsRDD.flatMap { x =>
      var list = new ArrayList[Tuple2[Long, Long]]
      val row = x._2
      val click_category_id = Option(row.getAs[Long]("click_category_id"))
      val order_product_ids = Option(row.getAs[String]("order_product_ids"))
      val pay_category_ids = Option(row.getAs[String]("pay_category_ids"))
      click_category_id.map(x => list.add((x.toLong, x.toLong)))
      order_product_ids.map { _.split(",") }.map(x => x.foreach { s => list.add((s.toLong, s.toLong)) })
      pay_category_ids.map { _.split(",") }.map(x => x.foreach { s => list.add((s.toLong, s.toLong)) })
      list.asScala
    }
    //      val top10Category = categoryidRDD.reduceByKey(_+_).top(10)
    //访问过的品类
    categoryidRDD = categoryidRDD.distinct()
    //点击过的品类
    val clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2DetailsRDD)
    //下单的
    val orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2DetailsRDD)
    //支付的
    val payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionId2DetailsRDD)
    //
    val categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD)
    categoryid2countRDD.take(10).foreach(println)

    val ordered = categoryid2countRDD.map { x =>
      val value = x._2
      val clickCount = StringUtils.getFieldFromConcatString(value, Constants.FIELD_CLICK_COUNT).toInt
      val orderCount = StringUtils.getFieldFromConcatString(value, Constants.FIELD_ORDER_COUNT).toInt
      val payCount = StringUtils.getFieldFromConcatString(value, Constants.FIELD_PAY_COUNT).toInt
      (SortKey(clickCount, orderCount, payCount), value)
    }
    //     ordered.sortByKey(false).take(10)
    val ordering = Ordering.by[(SortKey, String), SortKey](_._1)
    val top10Category = ordered.top(10)(ordering)
    top10Category

  }

  //  计算各个品类的点击次数
  def getClickCategoryId2CountRDD(sessionId2DetailsRDD: RDD[(String, Row)]): RDD[(Long, Long)] = {
    val clickActionRdd = sessionId2DetailsRDD.filter { x =>
      val row = x._2
      val click_category_id = Option(row.getAs[Long]("click_category_id"))
      click_category_id match {
        case Some(_) => true
        case None    => false
      }
    }
    val clickCategoryCount = clickActionRdd.map { x =>
      val row = x._2
      val click_category_id = Option(row.getAs[Long]("click_category_id"))
      (click_category_id.get, 1L)
    }.reduceByKey(_ + _)
    clickCategoryCount
  }
  //  计算各个品类的下单次数
  def getOrderCategoryId2CountRDD(sessionId2DetailsRDD: RDD[(String, Row)]): RDD[(Long, Long)] = {
    val orderActionRDD = sessionId2DetailsRDD.filter { x =>
      val row = x._2
      val click_category_id = Option(row.getAs[Long]("order_product_ids"))
      click_category_id match {
        case Some(_) => true
        case None    => false
      }
    }
    val orderCategoryId2CountRDD = orderActionRDD.flatMap { x =>
      val row = x._2
      val list = new ArrayList[Tuple2[Long, Long]]
      val ordrer_category_id = Option(row.getAs[String]("order_product_ids"))

      ordrer_category_id.get.split(",").map { x => list.add((x.toLong, 1L)) }
      list.asScala
    }.reduceByKey(_ + _)
    orderCategoryId2CountRDD
  }
  //  计算各个品类的支付次数
  def getPayCategoryId2CountRDD(sessionId2DetailRDD: RDD[(String, Row)]): RDD[(Long, Long)] = {
    val payActionRDD = sessionId2DetailRDD.filter { x =>
      val row = x._2
      val click_category_id = Option(row.getAs[Long]("pay_category_ids"))
      click_category_id match {
        case Some(_) => true
        case None    => false
      }
    }
    val payCategoryId2CountRDD = payActionRDD.flatMap { x =>
      val row = x._2
      val list = new ArrayList[Tuple2[Long, Long]]
      val ordrer_category_id = Option(row.getAs[String]("pay_category_ids"))

      ordrer_category_id.get.split(",").map { x => list.add((x.toLong, 1L)) }
      list.asScala
    }.reduceByKey(_ + _)
    payCategoryId2CountRDD
  }

  def joinCategoryAndData(categoryidRDD: RDD[(Long, Long)],
                          clickCategoryId2CountRDD: RDD[(Long, Long)],
                          orderCategoryId2CountRDD: RDD[(Long, Long)],
                          payCategoryId2CountRDD: RDD[(Long, Long)]): RDD[(Long, String)] = {
    var tmpJoin = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).map { x =>
      val categoryId = x._1
      val clickCount = x._2._2
      val value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount.getOrElse(0)

      (categoryId, value)
    }
    tmpJoin = tmpJoin.leftOuterJoin(orderCategoryId2CountRDD).map { x =>
      val categoryId = x._1
      var value = x._2._1
      val orderCount = x._2._2
      value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount.getOrElse(0)
      (categoryId, value)
    }
    tmpJoin = tmpJoin.leftOuterJoin(payCategoryId2CountRDD).map { x =>
      val categoryId = x._1
      var value = x._2._1
      val payCount = x._2._2
      value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount.getOrElse(0)
      (categoryId, value)
    }
    tmpJoin
  }

  def saveTop10Category2DB(taskid: Long, top10Category: Array[(SortKey, String)]) = {
    val top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
    for (tuple <- top10Category) {
      val countInfo = tuple._2;
      val categoryid = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong;
      val clickCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong;
      val orderCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong;
      val payCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong;

      val category = new Top10Category();
      category.setTaskid(taskid);
      category.setCategoryid(categoryid);
      category.setClickCount(clickCount);
      category.setOrderCount(orderCount);
      category.setPayCount(payCount);

      top10CategoryDAO.insert(category);
    }
  }
  def getTop10CategoryIDRdd(sc: SparkContext, top10Category: Array[(SortKey, String)]) = {
    val category = top10Category.map { x =>
      val categoryId = StringUtils.getFieldFromConcatString(x._2, Constants.FIELD_CATEGORY_ID).toLong
      (categoryId, categoryId)
    }
    val categoryRdd = sc.parallelize(category, 1)
    categoryRdd
  }

  /**
   * top10活跃session
   * top10热门品类，获取每个品类点击次数最多的10个session，以及其对应的访问明细
   */
  def getTop10Session(taskId: Long, sc: SparkContext, top10Category: Array[(SortKey, String)],
                      sessionId2DetailRDD: RDD[(String, Row)]) = {
    /**
     * 第一步：将top10热门品类的id，生成一份RDD
     */
    val top10CategoryIdList = getTop10CategoryIDRdd(sc, top10Category)

    /**
     * 第二步：计算top10品类被各session点击的次数
     */
    val session2DetailsRDD = sessionId2DetailRDD.groupByKey

    //categoryId,<sessionID,clickCount>
    val clickCategoryId2SessionCountRDD = session2DetailsRDD.flatMap { x =>
      val sessionId = x._1
      val iter = x._2
      //(categoryId,(sessionId,count))
      val list = new ArrayList[Tuple2[Long, Tuple2[String, Long]]]
      val map = new HashMap[Long, Long] //category,count
      for (row <- iter) {
        val click_category_id = Option(row.getAs[Long]("click_category_ids"))
        click_category_id.map { click_category_id =>
          Option(map.get(click_category_id)) match {
            case Some(_) => map.put(click_category_id, map.get(click_category_id) + 1)
            case None    => map.put(click_category_id, 1)
          }
        }
      }
      for (kv <- map.entrySet().iterator().asScala) {
        val categoryId = kv.getKey;
        val count = kv.getValue
        list.add((categoryId, (sessionId, count)))
      }
      list.asScala
    }
    //categoryId,<sessionID,clickCount> 
    //     val group : RDD[(Long, Iterable[(String, Long)])] =  clickCategoryId2SessionCountRDD.groupByKey()
    //     val top10SessionPerCategory =  group.flatMap{ x=>
    //       val  list = x._2.toList.sortBy(-_._2).take(10)
    //     }

    // 获取到to10热门品类，被各个session点击的次数
    //categoryId,<sessionID,clickCount>
    val top10CategorySessionCountRDD = top10CategoryIdList.join(clickCategoryId2SessionCountRDD).map(x => (x._1, x._2._2))

    //    top10CategorySessionCountRDD.sortBy(x=>x._2._2, false, 1)

    /**
     * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
     */
    val group = top10CategorySessionCountRDD.groupByKey()
    val top10Session = group.flatMap { x =>
      //获取每个品类的top10Session
      val sessionCounts = x._2.toList.sortBy(-_._2).take(10)

      for (sessionCount <- sessionCounts) {
        val sessionid = sessionCount._1
        val count = sessionCount._2
        // 将top10 session插入MySQL表
        val top10Session = new Top10Session();
        top10Session.setTaskid(taskId);
        top10Session.setCategoryid(x._1);
        top10Session.setSessionid(sessionid);
        top10Session.setClickCount(count);

        val top10SessionDAO = DAOFactory.getTop10SessionDAO();
        top10SessionDAO.insert(top10Session);
      }
      sessionCounts.toIterator
    }
    top10Session

  }

}