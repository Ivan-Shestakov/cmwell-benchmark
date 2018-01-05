package cmwell.benchmark.data

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import cmwell.benchmark.data.AllQueueStatus.extractResponse
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
  * Monitors CM-Well's persist/index queues.
  * The `Future` returned by `result` will return when queues have been empty for at least `waitForCancelCheck`.
  * This was originally created in an attempt to determine the rate that queues were capable of processing.
  *
  * TODO: Since we are using this only to determine when the queues have been drained, it doesn't need to return
  * the complete history.
  */
case class IngestionQueueMonitor(host: String,
                                 port: Int,
                                 waitBeforeStarting: FiniteDuration = 0.seconds,
                                 waitBeforeCancelCheck: FiniteDuration = 0.seconds,
                                 frequency: FiniteDuration = 1.second)
                                (implicit system: ActorSystem,
                                 executionContext: ExecutionContextExecutor,
                                 actorMaterializer: ActorMaterializer) {

  private val logger = LoggerFactory.getLogger(classOf[IngestionQueueMonitor])

  private val request = HttpRequest(
    method = GET,
    uri = s"http://$host:$port/proc/bg?format=json")

  private val poolClientFlow = Http().cachedHostConnectionPool[Int](host = host, port = port)

  private val tickSequence = Iterator from 1

  private val (cancellable: Cancellable, resultFuture: Future[Seq[Option[AllQueueStatus]]]) =
    Source.tick(waitBeforeStarting, frequency, NotUsed)
      .map(_ => request -> tickSequence.next())
      .via(poolClientFlow)
      .via(Flow.fromFunction(processResponse))
      .map(Source.fromFuture).flatMapConcat(identity)
      .toMat(Sink.seq)(Keep.both)
      .run()

  private val checkForEmptyQueuesAfter = System.currentTimeMillis + waitBeforeCancelCheck.toMillis

  private def processResponse(responseAndTick: (Try[HttpResponse], Int)): Future[Option[AllQueueStatus]] = responseAndTick match {
    case (Success(response: HttpResponse), tick: Int) => response match {

      case HttpResponse(status, _, body, _) if status.isSuccess =>

        logger.debug(s"Queue status received for tick $tick")

        body.dataBytes
          .runFold(ByteString.empty)((z, x) => z.concat(x))
          .map { bytes =>
            val state: AllQueueStatus = extractResponse(bytes, tick)

            if (System.currentTimeMillis > checkForEmptyQueuesAfter)
              if (state.allQueuesEmpty)
                cancellable.cancel()

            Some(state)
          }

      case HttpResponse(status, _, _, _) =>
        logger.warn(s"Queue status request failed for tick $tick with status $status")
        Future.successful(None) // Skip this observation
    }

    case (Failure(ex: Throwable), tick) =>
      logger.error(s"Queue monitor request failed for tick $tick with exception $ex")
      throw ex
  }

  def cancel: Boolean = cancellable.cancel()

  def result: Future[Seq[Option[AllQueueStatus]]] = resultFuture
}