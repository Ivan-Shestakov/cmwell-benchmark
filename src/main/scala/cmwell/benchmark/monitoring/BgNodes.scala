package cmwell.benchmark.monitoring

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.util.ByteString
import spray.json._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}

/**
  * Given an URL to ws, get the addresses of bg nodes.
  */
object BgNodes {

  def apply(host: String,
            port: Int)
           (implicit system: ActorSystem,
            executionContext: ExecutionContextExecutor,
            actorMaterializer: ActorMaterializer): Seq[String] = {

    val request = HttpRequest(
      method = GET,
      uri = s"http://$host:$port/proc/health-detailed?format=json")

    val responseFuture = Http().singleRequest(request)
    val response = Await.result(responseFuture, Duration.Inf)

    if (!response.status.isSuccess)
      throw new RuntimeException(s"Request failed with status ${response.status.intValue}")

    val body = Await.result(response.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map(_.utf8String), Duration.Inf)

    // looking for keys in the infoton fields such as: bg@127.0.0.1*
    val bgPattern = "bg@([^*]+)\\*?".r

    body.parseJson.asJsObject.fields("fields").asJsObject.fields.collect {
      case (bgPattern(address), _) => address
    }.toSeq
  }
}
