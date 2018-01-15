package cmwell.benchmark.monitoring

import javax.management.ObjectName
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{FiniteDuration, _}

case class CountMonitoring(time: Long, count: Long)

/** Monitor a JMX count metric until cancelled, and produce a sequence of observations. */
case class IngestionMonitoring(phase: String,
                               host: String, // of bg node
                               port: Int = 7196, // JMX port
                               metric: String, // MBean object name - must have a "Count" property
                               frequency: FiniteDuration = 1.second)
                              (implicit system: ActorSystem,
                               executionContext: ExecutionContextExecutor,
                               actorMaterializer: ActorMaterializer) {

  private val url = new JMXServiceURL(s"service:jmx:rmi:///jndi/rmi://$host:$port/jmxrmi")
  private val connector = JMXConnectorFactory.connect(url)
  private val server = connector.getMBeanServerConnection()
  private val metricObjectName = new ObjectName(metric)

  private val logger = LoggerFactory.getLogger(classOf[IngestionMonitoring])

  // This actor makes blocking calls to JMX, so use a separate dispatcher to bulkhead thread consumption.
  private implicit val blockingDispatcher = system.dispatchers.lookup("blocking-jmx-dispatcher")

  private var buffer = List.empty[CountMonitoring]
  @volatile private var cancelled = false
  private val tick = "tick"

  private val tickActor: ActorRef = system.actorOf(Props(new Actor {

    def receive = {
      case tick =>

        if (!cancelled)
          processTick()

        if (!cancelled)
          system.scheduler.scheduleOnce(frequency, tickActor, tick)
    }
  }))

  system.scheduler.scheduleOnce(frequency, tickActor, tick)

  private def processTick(): Unit = {

    try {
      val count = server.getAttribute(metricObjectName, "Count").asInstanceOf[Long]
      buffer = CountMonitoring(System.currentTimeMillis, count) :: buffer
    }
    catch {
      case ex: Exception =>
        logger.warn(s"Failed to collect metrics for $phase for host $host: $ex")
    }
  }

  def cancel(): Unit = cancelled = true

  def result: Seq[CountMonitoring] = buffer.reverse.toVector // Return in order that observations were made.
}
