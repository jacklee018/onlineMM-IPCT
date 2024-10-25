package busGps

import org.apache.spark.rdd.RDD
import utils.CommonUtils

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object BusCommonMethod {

  /**
   * Extract [Vehicle ID, Bus Route ID_Up-or-Down, Longitude, Latitude, Timestamp] from raw gps logged data
   */
  def extractGpsData(x: Array[String]): Array[String] = {
    try {
      val timeStamp = CommonUtils.utcTimeToTS(x(11)).toString
      var direction = "up"
      if(x(16).toInt == 2) direction = "down"
      Array(x(3), x(4)+"_"+direction, x(8), x(9),timeStamp)
    } catch {
      case e : Exception => {
        null
      }
    }
  }

  /**
   * -> Array[(Vehicle ID, Bus Route ID_Up-or-Down, Longitude, Latitude, Timestamp)]
   */
  def commonPretreatment(rdd: RDD[String], minInterval: Int, maxInterval: Int, minRecordCount: Int): RDD[ArrayBuffer[Array[String]]] = {
    rdd
      .map(_.split(","))
      // Filter unlocated data
      .filter(x => x.length > 16 && x(7).equals("0") && (x(16).toInt == 1 || x(16).toInt == 2))
      // -> (Vehicle ID, Bus Route ID_Up-or-Down, Longitude, Latitude, Timestamp)
      .map(BusCommonMethod.extractGpsData)
      .filter(_ != null)
      .groupBy(_(0))
      // -> Array[(Vehicle ID, Bus Route ID_Up-or-Down, Longitude, Latitude, Timestamp)]
      .map(_._2)
      .map(BusCommonMethod.sort_filter_split(_, minInterval, maxInterval, minRecordCount))
      // -> Array[(Vehicle ID, Bus Route ID_Up-or-Down, Longitude, Latitude, Timestamp)]
      .flatMap(x => x)
  }

  /**
   * First 5 recorded values：Vehicle ID, Bus Route ID_Up-or-Down, Longitude, Latitude, Timestamp
   */
  def sort_filter_split(gpsDataIter: Iterable[Array[String]], minInterval: Int, maxInterval: Int, minRecordCount: Int): ArrayBuffer[ArrayBuffer[Array[String]]] = {
    // Sort by time
    val gpsDataArray = gpsDataIter.toArray.sortBy(_ (4).toLong)
    val result = new ArrayBuffer[ArrayBuffer[Array[String]]]()
    var continuousDataArray = new ArrayBuffer[Array[String]]()
    continuousDataArray.append(gpsDataArray(0))
    var latestTime = gpsDataArray.head(4).toLong

    for (gpsData <- gpsDataArray) {
      val time = gpsData(4).toLong
      if ((time - latestTime) <= maxInterval * 1000) {
        if (isGpsDataExist(continuousDataArray, gpsData)) {

        } else if (time == latestTime) {
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
   * Check for the presence of data in the first 30 seconds of the continuousDataArray that are spaced apart from but in the same position as the gpsData
   * Keep only the first and last of consecutive identical ones
   */
  def isGpsDataExist(continuousDataArray: ArrayBuffer[Array[String]], gpsData: Array[String]): Boolean = {
    val size = continuousDataArray.size;
    if (size <= 1)
      return false
    if (continuousDataArray.last(2).equals(gpsData(2)) && continuousDataArray.last(3).equals(gpsData(3))) { //与最后一点位置相同
      if (!continuousDataArray(size-2)(2).equals(gpsData(2)) && continuousDataArray(size-2)(3).equals(gpsData(3))) { //与倒数第二个不同
        return false;
      } else {
        continuousDataArray.remove(size-1)
        return false
      }
    }
    val gTime = gpsData(4).toLong
    var index = continuousDataArray.length - 2
    var previousData = continuousDataArray(index)
    var time = previousData(4).toLong
    while (gTime - time < 30 * 1000) {
      if (previousData(2).equals(gpsData(2)) && previousData(3).equals(gpsData(3)))
        return true
      index -= 1
      if (index < 0)
        return false
      previousData = continuousDataArray(index)
      time = previousData(4).toLong
    }
    return false
  }
}
