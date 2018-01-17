package cmwell.benchmark.monitoring

import org.apache.commons.math3.stat.regression.SimpleRegression
import spray.json.DefaultJsonProtocol._
import spray.json._

case class IngestionResult(host: String,
                           phase: String,
                           infotons: Long,
                           rate: Double,
                           start: Long,
                           end: Long)

object IngestionResults {

  private implicit val ingestFormat: RootJsonFormat[IngestionResult] = jsonFormat6(IngestionResult)

  def toJson(results: Seq[IngestionResult]): String = {
    results.toJson.prettyPrint
  }

  def fromJson(source: String): Seq[IngestionResult] = {
    source.parseJson.convertTo[Seq[IngestionResult]]
  }

  val fileName = "ingest.json"

  /** Convert monitoring observations to a rate */
  def apply(host: String,
            phase: String, // persist | index
            infotons: Long,
            observations: Seq[CountMonitoring]): IngestionResult = {

    require(observations.nonEmpty)

    if (observations.isEmpty)
      throw new RuntimeException("No observations")

    // The monitoring would have started before and ended after data was being written,
    // so the parts at the beginning and end don't measure ingestion rates. The first and
    // last observation where it did change is likely to be partially in ingestion, so discard
    // that tool.
    val first = observations.indexWhere(_.count != observations.head.count) + 1
    val last = observations.lastIndexWhere(_.count != observations.last.count) - 1

    if (last - first < 2)
      throw new RuntimeException("Insufficient observations to calculate rate.")

    // The rate of processing should be linear (and it has been observed as such),
    // but any error in the first and last elements is amplified, and doesn't use any of the
    // observations in between.
    // Do a linear regression, and use the calculated slope to determine the rate.
    // This uses all the observations as input.

    //    {
    //      println(phase)
    //      val baseTime = observations.head.time
    //      val baseCount = observations.head.count
    //      for (ob <- observations)
    //        println(s"${ob.time - baseTime}, ${ob.count - baseCount}")
    //    }

    val baseTime = observations(first).time
    val baseCount = observations(first).count
    val regression = new SimpleRegression()
    for {
      i <- first to last
      observation = observations(i)
    } regression.addData(observation.time - baseTime, observation.count - baseCount)

    val slope = regression.getSlope

    // TODO: Test whether a linear model was fitted accurately (i.e., detect non-linear discontinuities in the data).
    //val slopeConfidence = regression.getSlopeConfidenceInterval

    // Since the unit on the x-axis is milliseconds, the slope represents a rate in milliseconds.
    IngestionResult(
      host = host,
      phase = phase,
      infotons = infotons,
      rate = slope * 1000, // infotons / second
      start = observations(first).time,
      end = observations(last).time)
  }
}
