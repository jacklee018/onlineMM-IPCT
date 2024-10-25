package busGps.paperExperiment

import MapMatchBaseline.STMatching.EnhanceSTMatcher3_O
import bfGpsDataMatch.ClientMapMatcher
import busGps.BusCommonMethod
import com.bmwcarit.barefoot.matcher.{MatcherCandidate, MatcherKState, MatcherSample}
import com.esri.core.geometry.Point
import common.CommonScalaMethod
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import utils.SystemUtils

import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, bufferAsJavaListConverter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Use sampled GPS points obtained via MatchInBusLine and correctly matched points
 * To test the method for correct matching at each level of sparsity
 */
object BaselineMatchTest2_EnhanceST3_O {

    var mapPath = "resources/overpass.bfmap"
    val dataPath = "data/MatchInBusLine2/part*"

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = CommonScalaMethod.buildSparkContext("BaselineMatchTest2")
    println("EnhanceSTMatcher3_O")
    val matcherBC: Broadcast[ClientMapMatcher[EnhanceSTMatcher3_O]] = sc.broadcast(new ClientMapMatcher[EnhanceSTMatcher3_O](mapPath, 50, classOf[EnhanceSTMatcher3_O]))
    val rdd: RDD[String] = sc.textFile(dataPath)
    println(rdd.count())
    val rate = Array((5, 15), (15, 25), (25, 35), (35, 45), (45, 55), (55, 65), (65, 75), (75, 85), (85, 95), (95, 105), (105, 115), (115, 125))
    var accuCandsNum: Long = 0
    var accuSampleNum: Long = 0
    rate.foreach(t => {
      val totalTimeAccumulator: LongAccumulator = sc.longAccumulator("TotalTimeAccumulator")
      var nullResult_cnt = 0
      val result: (Int, Int) = rdd.map(_.split(","))
        .groupBy(_ (0))
        .map(_._2)
        .map(BusCommonMethod.sort_filter_split(_, t._1, t._2, 10))
        .flatMap(x => x)
        .map { x =>
          val result_Mt = doBfMatch(x, matcherBC.value)
          // accumulate the operating time
          val result = (result_Mt._1, result_Mt._2)
          if(result == (0,0)){
            nullResult_cnt += 1
          }
          else totalTimeAccumulator.add(result_Mt._3)
          result
        }
        .filter(_ != null)
        .reduce((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))

      println("Sampling rate: " + t._1 + "-" + t._2 + "s")
      println(EnhanceSTMatcher3_O.candsNum.get(), EnhanceSTMatcher3_O.sampleNum.get())
      val candsNum = EnhanceSTMatcher3_O.candsNum.get() - accuCandsNum
      val sampleNum = EnhanceSTMatcher3_O.sampleNum.get() - accuSampleNum
      accuCandsNum = EnhanceSTMatcher3_O.candsNum.get()
      accuSampleNum = EnhanceSTMatcher3_O.sampleNum.get()
      println(candsNum, sampleNum)
      println("Average number of search roads: " + candsNum.toDouble / sampleNum)
      println(s"Total time spent on doBfMatch: ${totalTimeAccumulator.value / 1e6} ms")
      println("Mt: " + (totalTimeAccumulator.value / 1e6).toDouble / result._1.toDouble + "ms")
      println(result.toString())
      println("Prc: " + result._2.toDouble / result._1)
      println()
    })

    sc.stop()
  }


  def doBfMatch(gpsDataArray: mutable.Buffer[Array[String]], matcher: ClientMapMatcher[EnhanceSTMatcher3_O]): Tuple3[Int, Int, Long] = {
    val samples = new ListBuffer[MatcherSample]()

    // gps point id (associate the matching point with the original one)
    var id = 0
    for (gpsData <- gpsDataArray) {
      val sample = new MatcherSample(id.toString, gpsData(4).toLong, new Point(gpsData(2).toDouble, gpsData(3).toDouble))
      samples.append(sample)
      id = id + 1
    }

    // start matching
    val matchStartTime = System.nanoTime()
    val matcherKState: MatcherKState = matcher.mmatch(samples.asJava)
    val matchEndTime = System.nanoTime()
    val matchTime = matchEndTime - matchStartTime

    if (matcherKState == null) return (0, 0, matchTime)
    val candidates: util.List[MatcherCandidate] = matcherKState.sequence()
    if (candidates == null) return (0, 0, matchTime)
    var mCount = 0
    // count the right MM results
    for (c <- candidates.asScala) {
      val id: Int = c.sample().id().toInt
      val matchRoad: Long = c.point().edge().base().refid()
      if (gpsDataArray(id)(5).equals(matchRoad.toString))
        mCount += 1
    }
    (candidates.size(), mCount, matchTime)
  }
}
