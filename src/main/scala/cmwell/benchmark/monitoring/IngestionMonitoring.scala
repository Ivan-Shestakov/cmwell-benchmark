package cmwell.benchmark.monitoring

import javax.management.ObjectName
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContextExecutor, Future}

case class CountMonitoring(time: Long, count: Long)

case class IngestionMonitoring(phase: String,
                               host: String, // of bg node
                               port: Int = 7196, // JMX port
                               metric: String, // MBean object name - must have a "Count" property
                               frequency: FiniteDuration = 1.second) {

  private val url = new JMXServiceURL(s"service:jmx:rmi:///jndi/rmi://$host:$port/jmxrmi")
  private val connector = JMXConnectorFactory.connect(url)
  private val server = connector.getMBeanServerConnection()
  private val metricObjectName = new ObjectName(metric)

  private val logger = LoggerFactory.getLogger(classOf[IngestionMonitoring])

  // When this was implemented using the global akka context, cancelling one of these monitors cause the
  // others to fail. Not sure how to make cancel work gracefully, but isolating each of the monitors in its own
  // context seems to solve the problem.
  implicit val system: ActorSystem = ActorSystem("cmwell-benchmark")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  private val (cancellable: Cancellable, resultFuture: Future[Seq[Option[CountMonitoring]]]) =
    Source.tick(0.seconds, frequency, NotUsed)
      .map(_ => processTick).async
      .toMat(Sink.seq)(Keep.both)
      .run()

  private def processTick: Option[CountMonitoring] = {

    try {
      Some(CountMonitoring(System.currentTimeMillis, server.getAttribute(metricObjectName, "Count").asInstanceOf[Long]))
    }
    catch {
      case ex: Exception =>
        logger.warn(s"Failed to collect metrics: $ex")
        None
    }
  }

  def cancel(): Boolean = cancellable.cancel()

  def result: Future[Seq[Option[CountMonitoring]]] = resultFuture
}
