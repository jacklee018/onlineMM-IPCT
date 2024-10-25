package common

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import utils.{SystemUtils,CommonUtils}

object CommonScalaMethod {

  /**
   * On local Windows: setMaster("local[*]")，else needn't
   */
  def buildSparkContext(appName: String): SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName(appName)

    if (SystemUtils.isWindows) {
      sparkConf.setMaster("local[3]")
        .set("spark.driver.maxResultSize", "4g");
    }
    new SparkContext(sparkConf)
  }
  /**
   * Parses the timestamp, extracts the hour and minute, and calculates the time period number to which it belongs.
   */
  def getTimeSlot(time: Long): Int = {
    val str: String = CommonUtils.stampToTime(time, "HH:mm")
    val hour: Int = str.split(":")(0).toInt
    val minute: Int = str.split(":")(1).toInt
    // Calculate time difference, convert to minutes
    val minutesSinceDayStart: Int = hour * 60 + minute
    // Calculate timeslot
    val timeSlot: Int = minutesSinceDayStart / 5
    timeSlot
  }


  /**
   * Examine whether p is with the Rectangle marked by lbPoint and ruPoint
   * @param p
   * @param lbPoint left below
   * @param ruPoint right upper
   * @return
   */
  def inRectangle(p : GpsPoint, lbPoint : GpsPoint, ruPoint : GpsPoint): Boolean = {
    p.longitude >= lbPoint.longitude && p.longitude <= ruPoint.longitude && p.latitude >= lbPoint.latitude && p.latitude <= ruPoint.latitude
  }
}
