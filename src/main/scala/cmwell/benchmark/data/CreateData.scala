package cmwell.benchmark.data

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

/**
  * Initialize CM-Well with generated data.
  */
object CreateData {

  def apply(uri: Uri,
            path: String,
            chunks: Int,
            infotonsPerChunk: Int): Unit = {

    val logger = LoggerFactory.getLogger("benchmark")

    implicit val system: ActorSystem = ActorSystem("http-example")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    try {
      logger.info("Starting monitoring of background queues")
      // Monitoring will terminate when the queues have been empty for `waitForCancelCheck`

      val queueMonitorData: Future[Seq[Option[AllQueueStatus]]] = IngestionQueueMonitor(
        host = uri.authority.host.address,
        port = uri.authority.port,
        waitBeforeCancelCheck = 10.seconds,
        waitBeforeStarting = 0.seconds,
        frequency = 0.1.second).result

      logger.info("Starting POSTing infotons.")
      PostGeneratedData(
        host = uri.authority.host.address,
        port = uri.authority.port,
        path = path,
        chunks = chunks,
        infotonsPerChunk = infotonsPerChunk).run()

      // Wait for CM-Well to persist/index the data posted.
      val queueMonitoringData = Await.result(queueMonitorData, Duration.Inf).flatten
      logger.info("Generated data has been persisted/indexed.")

      // Here we are primarily concerned with detecting when the queues have been em
      // This was an attempt to measure the persist/index rates by looking at the queue monitoring data.
      // This produces highly variable results since the queue positions are only updated every 10 seconds.
      // Perhaps there are other values that can be monitored to determine the monitoring rate.
      //logger.info(s"Monitoring data:\n${AllQueueStatus.flattenAndGenerateCSV(queueMonitoringData)}")
      logger.info(IngestionRates(queueMonitoringData).toString)
    }
    finally {
      system.terminate()
    }
  }
}
