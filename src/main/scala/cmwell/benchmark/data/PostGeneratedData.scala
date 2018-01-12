package cmwell.benchmark.data

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.pattern.after
import akka.stream.ActorMaterializer
import akka.stream.contrib.Retry
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
  * Generates a sequence of infotons that are POSTed to CM-Well.
  */
case class PostGeneratedData(host: String,
                             port: Int,
                             path: String,
                             infotonCount: Long,
                             randomSeed: Int = 0)
                            (implicit system: ActorSystem,
                             executionContext: ExecutionContextExecutor,
                             actorMaterializer: ActorMaterializer) {

  import PostGeneratedData._

  private val poolClientFlow = Http().cachedHostConnectionPool[PostInfotonFlowState](host = host, port = port)

  private val generator = new InfotonGenerator(randomSeed, s"http://$host:$port/$path", infotonCount)

  private val postToUri = s"http://$host:$port/_in?format=ntriples"

  private val chunks = (infotonCount / infotonsPerChunk) + (if (infotonCount % infotonsPerChunk != 0) 1 else 0)

  private def infotonsInThisChunk(sequenceNumber: Long): Int =
    if (sequenceNumber == chunks)
      (infotonCount % infotonsPerChunk).toInt
    else
      infotonsPerChunk


  def run(): Future[Done] = Source(1L to chunks)
    .map(sequenceNumber => initialRequest(sequenceNumber))
    .via(Retry(postRequestFlow)(retryDecider))
    .runWith(Sink.ignore)

  private val postRequestFlow: Flow[(Future[HttpRequest], PostInfotonFlowState), (Try[HttpResponse], PostInfotonFlowState), NotUsed] =
    Flow[(Future[HttpRequest], PostInfotonFlowState)]
      .mapAsync(1) { case (input, state) => input.map(_ -> state) }
      .via(poolClientFlow)
      .map {
        // Transform a successful response with a 503 to a failure, so that it can be retried.
        case (Success(response), state: PostInfotonFlowState) if response.status.intValue == 503 =>
          logger.warn(s"POST for sequence=${state.sequenceNumber} failed with status 503.")
          response.discardEntityBytes()
          Failure(exception) -> state

        case (Success(response), state: PostInfotonFlowState) =>
          logger.debug(s"POST for sequence=${state.sequenceNumber} succeeded.")
          response.discardEntityBytes()
          Success(response) -> state

        case (Failure(ex), state: PostInfotonFlowState) =>
          logger.error(s"Posting data failed for sequence=${state.sequenceNumber} with $ex")
          throw ex
      }

  private def initialRequest(sequenceNumber: Long): (Future[HttpRequest], PostInfotonFlowState) = {
    val (contentLength: Int, infotons: List[ByteString]) = (0 until infotonsInThisChunk(sequenceNumber))
      .foldLeft(0 -> List.empty[ByteString]) { case ((currentContentLength, currentInfotons), _) =>
        val data = ByteString(generator.next())
        (currentContentLength + data.length) -> (data :: currentInfotons)
      }

    val entity = HttpEntity.Default(
      contentType = ContentTypes.`text/plain(UTF-8)`,
      contentLength = contentLength,
      data = Source(infotons))

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = postToUri,
      entity = entity)

    Future.successful(request) -> PostInfotonFlowState(request = request, sequenceNumber = sequenceNumber)
  }

  private def retryDecider(state: PostInfotonFlowState): Option[(Future[HttpRequest], PostInfotonFlowState)] = {
    logger.warn(s"Final POST retry for sequence number ${state.sequenceNumber} failed - this POST will be dropped.")
    Some(after(retryWaitSeconds.seconds, system.scheduler)(Future.successful(state.request)) -> state)
  }
}

object PostGeneratedData {
  val infotonsPerChunk = 1000
  val logger: Logger = LoggerFactory.getLogger(classOf[PostGeneratedData])
  val retryWaitSeconds = 10
  val exception = new Exception()
}

case class PostInfotonFlowState(request: HttpRequest, sequenceNumber: Long)
