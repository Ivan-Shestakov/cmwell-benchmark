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
      Await.result(queueMonitorData, Duration.Inf)
      logger.info("Generated data has been persisted/indexed.")
    }
    finally {
      system.terminate()
    }
  }
}
