package cmwell.benchmark

import java.time.ZonedDateTime

import akka.util.ByteString
import spray.json._

import scala.collection.mutable

/**
  * The status of one of the queues.
  * The data source will contain separate data from each partition, but it is aggregated to a total for all partitions.
  * The lag is dropped since it is redundant (can be calculated from readOffset and writeOffset).
  */
case class QueueStatus(status: String,
                       readOffset: Long,
                       writeOffset: Long) {

  def isEmpty: Boolean = readOffset == writeOffset
}

case class AllQueueStatus(time: Long,
                          tick: Int,
                          index: QueueStatus,
                          indexPriority: QueueStatus,
                          persist: QueueStatus,
                          persistPriority: QueueStatus) {

  def allQueuesEmpty: Boolean = index.isEmpty && indexPriority.isEmpty && persist.isEmpty && persistPriority.isEmpty
}

object AllQueueStatus {

  def extractResponse(body: ByteString, tick: Int): AllQueueStatus = {
    val js: JsObject = body.utf8String.parseJson.asJsObject

    val time = ZonedDateTime.parse(js.fields("system").asJsObject.fields("lastModified").asInstanceOf[JsString].value)
      .toInstant.toEpochMilli

    val fields: JsObject = js.fields("fields").asJsObject

    def extractStrings(name: String) = fields.fields(name).asInstanceOf[JsArray]
      .elements.map(element => element.asInstanceOf[JsString].value)

    def extractLongs(name: String) = fields.fields(name).asInstanceOf[JsArray]
      .elements.map(element => element.asInstanceOf[JsNumber].value.toLong)

    def extractQueueStatus(status: String, read: String, write: String, lag: String) =
      QueueStatus(status = extractStrings(status).reduce(maxStatus),
        readOffset = extractLongs(read).sum,
        writeOffset = extractLongs(write).sum)

    AllQueueStatus(
      time = time,
      tick = tick,
      index = extractQueueStatus(
        "index_topic_status",
        "index_topic_read_offset",
        "index_topic_write_offset",
        "index_topic_lag"),
      indexPriority = extractQueueStatus(
        "index_topic.priority_status",
        "index_topic.priority_read_offset",
        "index_topic.priority_write_offset",
        "index_topic.priority_lag"),
      persist = extractQueueStatus(
        "persist_topic_status",
        "persist_topic_read_offset",
        "persist_topic_write_offset",
        "persist_topic_lag"),
      persistPriority = extractQueueStatus(
        "persist_topic.priority_status",
        "persist_topic.priority_read_offset",
        "persist_topic.priority_write_offset",
        "persist_topic.priority_lag"))
  }

  private def makeHeaders(queue: String) =
    Seq("status", "readOffset", "writeOffset").map(field => s"${queue}_$field").mkString(",")

  private def maxStatus(status1: String, status2: String) =
    if (status1 == "Red" || status2 == "Red")
      "Red"
    else if (status1 == "Yellow" || status2 == "Yellow")
      "Yellow"
    else
      "Green"

  // This is provided for generating data that could be graphed or analyzed externally.
  def flattenAndGenerateCSV(s: Seq[AllQueueStatus]): String = {

    val sb = new mutable.StringBuilder()

    sb ++= s"""time,tick,${makeHeaders("index")},${makeHeaders("indexPriority")},${makeHeaders(s"persist")},${makeHeaders("persistPriority")}\n"""

    def format(qs: QueueStatus) = s"${qs.status},${qs.readOffset},${qs.writeOffset}"

    for (row <- s) {
      sb ++= s"${row.tick},${row.time}," // TODO: Convert time to printable format?
      sb ++= s"${format(row.index)},${format(row.indexPriority)},${format(row.persist)},${format(row.persistPriority)}\n"
    }

    sb.toString
  }
}

/** The processing rate for each queue, measured as infotons per millisecond */
case class IngestionRates(persist: Double, index: Double)

object IngestionRates {

  /**
    * Given a sequence of AllQueueStatus, calculate the rate at which each queue can process infotons.
    * The problem is that BGMonitorActor is refreshing at some rate (currently 10 seconds), but that is not
    * necessarily in sync with the monitoring is done. If we assume that the monitoring and is done at some rate
    * that is a multiple of the BGMonitorActor refresh rate, then we can must look at the first and last period
    * where there is a change in the queue, then the processing rate can be calculated directly.
    *
    * The accuracy could be improved by tweaking BGMonitorActor to attempt to regularize the frequency to a multiple
    * of its desired frequency (to avoid drift).
    *
    * This also assumes that there is no point at the queue is empty between the periods where it starts/stops
    * processing. We can provide some validation of this, but the validation is not complete.
    *
    * This only works if indexing takes longer than persistence. Otherwise, indexing would drain its queues
    * and become idle at some point during ingestion.
    */
  def apply(s: Seq[AllQueueStatus]): IngestionRates = {

    def firstWithChange(extractQueue: AllQueueStatus => QueueStatus): Int =
      s.indexWhere(allQueueStatus => !extractQueue(allQueueStatus).isEmpty)

    def lastWithChange(extractQueue: AllQueueStatus => QueueStatus): Int =
      s.lastIndexWhere(allQueueStatus => !extractQueue(allQueueStatus).isEmpty)

    def calculateRate(extractQueue: AllQueueStatus => QueueStatus): Double = {
      val start = firstWithChange(extractQueue)
      val finish = lastWithChange(extractQueue)

      // Check that the queue is never empty during the period of calculation.
      (start to finish).foreach(i => assert(!extractQueue(s(i)).isEmpty))

      // TODO: Check that there is some data ingested during the period of calculation.

      // TODO: Check that start != finish, otherwise, there is nothing to measure

      val deltaTime = s(finish).time - s(start).time
      val deltaInfoton = extractQueue(s(finish)).readOffset - extractQueue(s(start)).readOffset

      deltaInfoton.toDouble / deltaTime
    }

    val index = (x: AllQueueStatus) => x.index
    val persist = (x: AllQueueStatus) => x.persist

    IngestionRates(
      persist = calculateRate(persist),
      index = calculateRate(index))
  }
}