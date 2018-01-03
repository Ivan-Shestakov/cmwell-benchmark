package cmwell.benchmark

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.contrib.Retry
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}
import akka.pattern.after

case class PostInfotonFlowState(request: HttpRequest,
                                sequenceNumber: Int,
                                retriesLeft: Int)

case class PostGeneratedData(host: String,
                             port: Int,
                             path: String,
                             chunks: Int,
                             infotonsPerChunk: Int,
                             randomSeed: Int = 0)
                            (implicit system: ActorSystem,
                             executionContext: ExecutionContextExecutor,
                             actorMaterializer: ActorMaterializer) {

  private val logger = LoggerFactory.getLogger(classOf[PostGeneratedData])

  private val poolClientFlow = Http().cachedHostConnectionPool[PostInfotonFlowState](host = host, port = port)

  private val generator = new Generator(randomSeed, path)

  private val postToUri = s"http://$host:$port/_in?format=ntriples"

  // TODO: Make configurable; implement (exponential) back-off strategy
  private val Retries = 10
  private val WaitSeconds = 30

  private val postRequestFlow: Flow[(Future[HttpRequest], PostInfotonFlowState), (Try[HttpResponse], PostInfotonFlowState), NotUsed] =
    Flow[(Future[HttpRequest], PostInfotonFlowState)]
      .mapAsync(1) { case (input, state) => input.map(_ -> state) }
      .via(poolClientFlow)
      .map {
        // Transform a successful response with a 503 to a failure, so that it can be retried.
        case (Success(response), state: PostInfotonFlowState) if response.status.intValue == 503 =>
          logger.warn(s"POST for sequence=${state.sequenceNumber} failed with status 503.")
          response.discardEntityBytes()
          Failure(new Throwable()) -> state // TODO: Need a particular throwable here?

        case (Success(response), state: PostInfotonFlowState) =>
          logger.debug(s"POST for sequence=${state.sequenceNumber} succeeded.")
          response.discardEntityBytes()
          Success(response) -> state

        case (Failure(ex), state: PostInfotonFlowState) =>
          logger.error(s"Posting data failed for sequence=${state.sequenceNumber} with $ex")
          throw ex

        case _ =>
          throw new Exception()
      }

  private def initialRequest(sequenceNumber: Int): (Future[HttpRequest], PostInfotonFlowState) = {
    val (contentLength: Int, infotons: List[ByteString]) = (0 until infotonsPerChunk)
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

    Future.successful(request) -> PostInfotonFlowState(request = request, sequenceNumber = sequenceNumber, retriesLeft = Retries)
  }

  def run(): Future[Done] = Source(1 to chunks)
    .map(sequenceNumber => initialRequest(sequenceNumber))
    .via(Retry(postRequestFlow)(retryDecider))
    .runWith(Sink.ignore)

  private def retryDecider(state: PostInfotonFlowState): Option[(Future[HttpRequest], PostInfotonFlowState)] =
    if (state.retriesLeft == 0) {
      logger.warn(s"Retrying POST for sequence number ${state.sequenceNumber}")
      None
    }
    else {
      logger.warn(s"Final POST retry for sequence number ${state.sequenceNumber} failed - this POST will be dropped.")
      Some(after(WaitSeconds.seconds, system.scheduler)(Future.successful(state.request)) -> state.copy(retriesLeft = state.retriesLeft - 1))
    }
}
