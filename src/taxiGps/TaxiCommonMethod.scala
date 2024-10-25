package taxiGps

import bfGpsDataMatch.BfMatchUtils
import com.bmwcarit.barefoot.matcher.{Matcher, MatcherKState, MatcherSample}
import com.esri.core.geometry.Point
import common.GpsMatchPoint
import org.apache.spark.rdd.RDD
import utils.CommonUtils

import scala.collection.JavaConverters.bufferAsJavaListConverter
import java.util
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * 处理出租车数据的公共方法
 */
object TaxiCommonMethod {

  val GPS_2017_TYPE = 17
  val GPS_2020_TYPE = 20

  val GPS_NEW_TYPE = 1

  /**
   * 由于各个时间段的gps数据格式都不一样，故写个公共方法在这
   * 从原始gps记录数据中提取出[车牌号，经度，纬度，时间戳，速度]
   */
  def extract2017gpsData(x: Array[String]): Array[String] = {
    try {
      val timeStamp = CommonUtils.utcTimeToTS(x(5)).toString
      Array(x(0), x(3), x(4), timeStamp, x(7))
    } catch {
      case e : Exception => {
        null
      }
    }
  }

  def extract2020gpsData(x: Array[String]): Array[String] = {
    try {
      val timeStamp = CommonUtils.utcTimeToTS(x(3)).toString
      Array(x(0), x(1), x(2), timeStamp, x(5));
    } catch {
      case e : Exception => {
        null
      }
    }
  }

  /**
   * 从原始gps记录数据中提取出[车牌号，经度，纬度，时间戳，GPS速度]
   */
  def newGpsData(x: Array[String]): Array[String] = {
    try {
      val timeStamp = CommonUtils.timeToTS(x(4), "yyyy-MM-dd HH:mm:ss").toString
      Array(x(1), x(2), x(3), timeStamp, x(6));
    } catch {
      case e : Exception => {
        null
      }
    }
  }

  /**
   * 通用出租车gps数据预处理方法：先分组 然后清洗 最后分段
   * @return
   */
  def commonPretreatment(rdd: RDD[String], dataType: Int, minInterval: Int, maxInterval: Int, minRecordCount: Int): RDD[ArrayBuffer[Array[String]]] = {
    rdd.map(_.split(","))
      //将数据格式转换为[车牌号，经度，纬度，时间，速度]
      .map(x => {
        dataType match {
          case GPS_NEW_TYPE => newGpsData(x)
          case GPS_2017_TYPE => extract2017gpsData(x)
          case GPS_2020_TYPE => extract2020gpsData(x)
        }
      })
      .filter(_ != null)
      //按车牌号将gps记录分组
      .groupBy(_(0))
      .map(x => x._2)
      //将一个车牌号的gps记录按时间段分为多组
      .map(sort_filter_split(_, minInterval, maxInterval, minRecordCount))
      //展平
      .flatMap(x => x)
  }

  //------------------------以下方法输入的gps数据格式均需为：[车牌号，经度，纬度，时间戳.....

  /**
   * 将一辆车的gps记录分组，时间间隔小于等于maxInterval秒的连续记录划分到一组（并过滤60秒内的相同位置数据），记录数少于minRecordCount的组舍弃
   *
   * @param minInterval    组数据最小时间间隔 单位：秒
   * @param maxInterval    组数据最大时间间隔 单位：秒
   * @param minRecordCount 一组数据的最小长度
   * @return
   */
  def sort_filter_split(gpsDataIter: Iterable[Array[String]], minInterval: Int, maxInterval: Int, minRecordCount: Int): ArrayBuffer[ArrayBuffer[Array[String]]] = {
    //根据时间排序
    val gpsDataArray = gpsDataIter.toArray.sortBy(_ (3).toLong)
    val result = new ArrayBuffer[ArrayBuffer[Array[String]]]()
    var continuousDataArray = new ArrayBuffer[Array[String]]()
    continuousDataArray.append(gpsDataArray(0))
    var latestTime = gpsDataArray.head(3).toLong

    for (gpsData <- gpsDataArray) {
      val time = gpsData(3).toLong
      if ((time - latestTime) <= maxInterval * 1000) {
        //1.过滤60秒内重复位置数据
        //        if (isGpsDataExist(continuousDataArray, gpsData)) {
        //
        //        } else
        //2.相同时间点数据只保留最后一条 3.最小时间间隔minInterval秒
        if (time == latestTime) {
          continuousDataArray(continuousDataArray.length - 1) = gpsData
        } else if ((time - latestTime) >= minInterval * 1000) {
          continuousDataArray.append(gpsData)
          latestTime = time
        }
      } else {
        if (continuousDataArray.length >= minRecordCount) {
          result.append(continuousDataArray)
        }
        continuousDataArray = new ArrayBuffer[Array[String]]()
        continuousDataArray.append(gpsData)
        latestTime = time
      }
    }

    if (continuousDataArray.length >= minRecordCount) {
      result.append(continuousDataArray)
    }
    result
  }

  /**
   * 检查continuousDataArray中前60秒中是否存在相同位置的数据
   */
  def isGpsDataExist(continuousDataArray: ArrayBuffer[Array[String]], gpsData: Array[String]): Boolean = {
    if (continuousDataArray.isEmpty)
      return false
    val gTime = gpsData(3).toLong
    var index = continuousDataArray.length - 1
    var previousData = continuousDataArray(index)
    var time = previousData(3).toLong
    while (gTime - time < 60 * 1000) {
      if (previousData(1).equals(gpsData(1)) && previousData(2).equals(gpsData(2)))
        return true
      index -= 1
      if (index < 0)
        return false
      previousData = continuousDataArray(index)
      time = previousData(3).toLong
    }
    return false
  }

  def doBfMatch(gpsDataIter: Iterable[Array[String]], matcher: Matcher): (Iterable[Array[String]], util.List[GpsMatchPoint]) = {
    val gpsDataArray = gpsDataIter.toSeq.sortBy(_(2).toLong)
    val samples = new ListBuffer[MatcherSample]()

    // gps point id (associate the matching point with the original one)
    var id = 0
    for (gpsData <- gpsDataArray) {
      val sample = new MatcherSample(id.toString, gpsData(3).toLong, new Point(gpsData(1).toDouble, gpsData(2).toDouble))
      samples.append(sample)
      id = id + 1
    }

    // start matching
    val matcherKState: MatcherKState = matcher.mmatch(samples.asJava, 0, 0)

    (gpsDataArray, BfMatchUtils.convertMatcherCandidates(matcherKState))
  }

  /**
   * 将gpsData中第四个字段utc时间转换为时间戳
   * 若格式不正确则返回null
   */
  def convertGpsUtcToTs(gpsData: Array[String]): Array[String] = {
    try {
      gpsData(3) = CommonUtils.utcTimeToTS(gpsData(3)).toString
      gpsData
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }
}
