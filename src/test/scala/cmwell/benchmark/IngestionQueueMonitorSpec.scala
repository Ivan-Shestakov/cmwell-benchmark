package cmwell.benchmark

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class IngestionQueueMonitorSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("http-example")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  override def afterAll: Unit = system.terminate

  val url = "http://localhost:9000"

  val ingestionQueueMonitor = IngestionQueueMonitor(host = "localhost", port = 9000, frequency = 1.second, waitBeforeCancelCheck = 5.seconds)

  val observations: Seq[Option[AllQueueStatus]] = Await.result(ingestionQueueMonitor.result, 20.seconds)

  "The result of monitoring" should "not be empty" in {
    observations should not be empty
  }

  it should "not have any missed observations" in {
    observations.forall(_.isDefined)
  }

  for (observation <- observations)
    println(observation.get)
}