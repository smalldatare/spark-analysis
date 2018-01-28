package org.richardqiao.scala.spark.ad

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.richardqiao.scala.test.StreamingExamples
import org.richardqiao.java.conf.ConfigurationManager
import org.richardqiao.java.constant.Constants
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import java.util.Date
import org.richardqiao.java.util.DateUtils
import org.richardqiao.java.domain.AdUserClickCount
import java.util.ArrayList
import org.richardqiao.java.dao.IAdUserClickCountDAO
import org.richardqiao.java.dao.factory.DAOFactory
import org.richardqiao.java.domain.AdBlacklist
import org.apache.spark.streaming.dstream.DStream
import org.richardqiao.java.domain.AdStat
import org.richardqiao.java.dao.IAdStatDAO
import org.richardqiao.java.domain.AdProvinceTop3
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Minutes
//数据格式介绍：
//
//timestamp 1450702800
//province  Jiangsu    535125724
//city  Nanjing
//userid  100001
//adid  100001

object AdClickRealTimeStatSpark {
  def main(args: Array[String]): Unit = {
//    if (args.length < 5) {
//      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads><masterUrl>")
//      System.exit(1)
//    }

    StreamingExamples.setStreamingLogLevels()

//    val Array(zkQuorum, group, numThreads, master) = args

    val conf = new SparkConf().setAppName("AdClickRealTimeStatSpark").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("checkpoint")

    //  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val topics = kafkaTopics.split(",").toSet
    val kafkaParams = Map(Constants.KAFKA_METADATA_BROKER_LIST -> ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST),
                          "metadata.broker.list" -> ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST))
    val adRealTimeLogDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val filterAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream)
    // 生成动态黑名单
    generateDynamicBlacklist(filterAdRealTimeLogDStream)

    // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount） 
    // 最粗
    val adRealTimeStatDStream = calculateRealTimeStat(filterAdRealTimeLogDStream)

    // 业务功能二：实时统计每天每个省份top3热门广告
    // 统计的稍微细一些了
    getTop3AdPerProvince(adRealTimeStatDStream)

    // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
    // 统计的非常细了
    // 我们每次都可以看到每个广告，最近一小时内，每分钟的点击量
    // 每支广告的点击趋势

    ssc.start()
    ssc.awaitTermination()
  }

  //过滤黑名单
  // 刚刚接受到原始的用户点击行为日志之后
  // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
  // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
  def filterByBlacklist(adRealTimeLogDStream: InputDStream[(String, String)]): DStream[(String, String)] = {
    val filterAdRealTimeLogDStream = adRealTimeLogDStream.transform(logRDD => {
      logRDD.mapPartitions(iter => {
        val adBlacklistDao = DAOFactory.getAdBlacklistDAO()
        val adBlacklist = adBlacklistDao.findAll()
        iter.filterNot(x => {
          val log = x._2;
          val logSplited = log.split(" ");
          val userid = logSplited(3)
          adBlacklist.contains(userid)
        })
      })
    })
    filterAdRealTimeLogDStream
  }

  // 生成动态黑名单
  def generateDynamicBlacklist(filterAdRealTimeLogDStream: DStream[(String, String)]) {
    val dailyUserAdClickDStream = filterAdRealTimeLogDStream.map { x =>
      val log = x._2
      val logSplited = log.split(" ")
      //timestamp 1450702800
      //province  Jiangsu    535125724
      //city  Nanjing
      //userid  100001
      //adid  100001
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val datekey = DateUtils.formatDateKey(date)
      val userid = logSplited(3)
      val adid = logSplited(4)
      val key = datekey + "_" + userid + "_" + adid
      (key, 1)
    }
    //  //每个batch中
    //
    val dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(_ + _)

    dailyUserAdClickCountDStream.foreachRDD { rdd =>
      rdd.foreachPartition { iter: Iterator[(String, Int)] => {
        val list = new ArrayList[AdUserClickCount]
        while (iter.hasNext) {
          val tuple = iter.next()
//          println(tuple._1 + "," + tuple._2)
          val adUserClickCount = new AdUserClickCount(tuple._1, tuple._2)
          list.add(adUserClickCount)
        }
        val adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
        adUserClickCountDAO.updateBatch(list)
      }
      }
    }
    //dailyUserAdClickCountDStream  聚合过，数据量减少了
    val blacklistDStram = dailyUserAdClickCountDStream.filter ( x =>{
      val key = x._1
      val keySplited = key.split("_")
      val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
      val userid = keySplited(1).toLong
      val adid = keySplited(2).toLong
      val adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO()
      val clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userid, adid)
      //如果点击量大于100，拉入黑名单
      if (clickCount + x._2 > 100) true
      //否则，暂时不管它
      else false
    })
    // balcklistDStram 就是过滤出来对某个广告点击量超过100的用户。
    //遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
    //一旦增加以后，在整个这段程序前面，会加上根据署名单动态过滤用户的逻辑
    //我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里
    //所以直接插入mysql
    val blacklistUserDStream = blacklistDStram.map(x => {
      val key = x._1
      val keySplited = key.split("_")
      val userid = keySplited(1).toLong
      userid
    })
    val distinctBlacklistUserDStream = blacklistUserDStream.transform(rdd => rdd.distinct())

    distinctBlacklistUserDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val list = new ArrayList[AdBlacklist]
        for (i <- iter) {
          list.add(new AdBlacklist(i))
        }
        val adBlacklistDao = DAOFactory.getAdBlacklistDAO()
        adBlacklistDao.insertBatch(list)
      })
    })
  }

  /**
    * //
    * //timestamp 1450702800
    * //province  Jiangsu    535125724
    * //city  Nanjing
    * //userid  100001
    * //adid  100001
    *
    * @param filterAdRealTimeLogDStream
    * @return
    */
  def calculateRealTimeStat(filterAdRealTimeLogDStream: DStream[(String, String)]): DStream[(String, Long)] = {
    def updateFunc: (Seq[Long], Option[Long]) => Some[Long] = (values: Seq[Long], state: Option[Long]) => {
      val currentCount = values.foldLeft(0L)(_ + _)
      val previous = state.getOrElse(0L)
      Some(currentCount + previous)
    }

    val aggregatedDStream = filterAdRealTimeLogDStream.map(x => {
      val log = x._2
      val logSplited = log.split(" ")
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val datekey = DateUtils.formatDateKey(date)
      val province = logSplited(1)
      val city = logSplited(2)
      val adid = logSplited(4)
      val key = datekey + "_" + province + "_" + city + "_" + adid
      (key, 1L)
    }).updateStateByKey(updateFunc)
    aggregatedDStream.foreachRDD(rdd => {
      rdd.foreachPartition { iter: Iterator[(String, Long)] => {
        val adStats = new ArrayList[AdStat]
        for (tuple <- iter) {
          val keySplited = tuple._1.split("_")
          val date = keySplited(0)
          val province = keySplited(1)
          val city = keySplited(2)
          val adid = keySplited(3).toLong
          val adStat = new AdStat(date, province, city, adid, tuple._2)
          adStats.add(adStat)
        }
        val adStatDAO = DAOFactory.getAdStatDAO();
        adStatDAO.updateBatch(adStats);
      }
      }

    })
    aggregatedDStream
  }


  /**
    * 业务功能二：实时统计每天每个省份top3热门广告
    * 统计的稍微细一些了
    *
    * @param statDstream
    */
  def getTop3AdPerProvince(statDstream: DStream[(String, Long)]) = {
    val top3 = statDstream.transform(rdd => {
      val ordering = Ordering.by[(String, Long), Long](_._2)
      val top = rdd.map { x =>
        val Array(day, province, _, adid) = x._1.split("_")
        val count = x._2
        val key = day + "_" + province + "_" + adid
        (key, count)
      }.reduceByKey(_ + _).top(3)(ordering)
      rdd.sparkContext.parallelize(top, 10)
    })

    // 每次都是刷新出来各个省份最热门的top3广告
    // 将其中的数据批量更新到MySQL中
    top3.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val adProvinceTop3List = new ArrayList[AdProvinceTop3]
        for (tuple <- iter) {
          val Array(day, province, adid) = tuple._1.split("_")
          val count = tuple._2

          val adProvinceTop3 = new AdProvinceTop3(day, province, adid.toLong, count)
          adProvinceTop3List.add(adProvinceTop3)
        }
        val adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
        adProvinceTop3DAO.updateBatch(adProvinceTop3List);
      })

    })
  }

  /**
    * 计算最近1小时滑动窗口内的广告点击趋势
    *
    * @param adRealTimeStatDStream
    *
    *
    * 旧的tuple格式
    * val key = datekey +"_"+ province + "_" +city +"_"+adid
    * (key,1L)
    */
  def calculateAdClickCountByWindow(adRealTimeStatDStream: DStream[(String, String)]) = {
    val mapDStream = adRealTimeStatDStream.map(x => {
      val log = x._2
      val Array(time, _, _, adid) = log.split("_")
      val date = DateUtils.formatTimeMinute(DateUtils.parseTime(time));
      (date + "_" + adid, 1L)
    })
    // adRealTimeStatDStream.reduceByKeyAndWindow((x:Long,y:Long)=>x+y, Minutes(60), Seconds(10))
    val window = mapDStream.reduceByKeyAndWindow(_ + _, _ - _, Minutes(60), Seconds(10))
  }
}