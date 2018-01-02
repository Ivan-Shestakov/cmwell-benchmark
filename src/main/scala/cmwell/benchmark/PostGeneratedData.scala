package cmwell.benchmark

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

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

  private val poolClientFlow = Http().cachedHostConnectionPool[Int](host = host, port = port)

  private val generator = new Generator(randomSeed, path)

  private val postToUri = s"http://$host:$port/_in?format=ntriples"

  def run(): Future[Done] = Source(1 to chunks)
    .map { sequenceNumber =>

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

      request -> sequenceNumber
    }
    .via(poolClientFlow)
    .runForeach {
      case (Success(response), sequenceNumber) =>
        response.discardEntityBytes()

        if (response.status.isSuccess) {
          logger.debug(s"POST for sequence=$sequenceNumber succeeded.")
        }
        else {
          // TODO: Implement a RestartSource.withBackoff to handle status 503
          // TODO: Might want to log the response entity (error message)
          logger.warn(s"POST for sequence=$sequenceNumber failed with status = ${response.status.intValue}")
        }

      case (Failure(ex), sequenceNumber) =>
        logger.error(s"Posting data failed for sequence=$sequenceNumber with $ex")
        throw ex
    }
}
