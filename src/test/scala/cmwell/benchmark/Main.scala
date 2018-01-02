package cmwell.benchmark

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Random

import org.slf4j.LoggerFactory

object Main extends App {

  override def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("benchmark")

    implicit val system: ActorSystem = ActorSystem("http-example")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    // TODO: Make these parameters
    val url = "http://localhost:9000"
    val chunks = 1000
    val infotonsPerChunk = 1000

    val uri = Uri(url)

    require(uri.scheme == "http", "URL scheme must be HTTP (configuring connection pool for https is not implemented yet).")

    // Generate a path so that we are generating data into an empty path.
    val path = s"http://${uri.authority.host}:${uri.authority.port}/benchmark-${new Random().nextInt()}"
    logger.info(s"Infotons will be generated in path $path")

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

      val queueMonitoringData = Await.result(queueMonitorData, Duration.Inf)
          .flatten  // Remove any missing observations and flatten

      logger.info(s"Monitoring data:\n${AllQueueStatus.flattenAndGenerateCSV(queueMonitoringData)}")

      println(IngestionRates(queueMonitoringData))
    }
    finally {
      system.terminate()
    }
  }
}
