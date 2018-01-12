package cmwell.benchmark.data

import akka.Done
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
            infotonCount: Long)
           (implicit system: ActorSystem,
            executionContext: ExecutionContextExecutor,
            actorMaterializer: ActorMaterializer): Unit = {

    val logger = LoggerFactory.getLogger("benchmark")

    try {
      logger.info("Starting monitoring of background queues")
      // Monitoring will terminate when the queues have been empty for `waitForCancelCheck`

      val queueMonitorData: Future[Done] = IngestionQueueMonitor(
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
        infotonCount = infotonCount).run()

      // Wait for CM-Well to persist/index the data posted.
      Await.result(queueMonitorData, Duration.Inf)
      logger.info("Generated data has been persisted/indexed.")
    }
    finally {
      system.terminate()
    }
  }
}
