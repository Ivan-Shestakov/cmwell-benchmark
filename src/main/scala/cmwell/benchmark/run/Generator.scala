package cmwell.benchmark.run

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import cmwell.benchmark.data._
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor

/**
  * This entry point generates data, but doesn't do any ingestion monitoring or run gatling simulations.
  */
object Generator extends App {

  override def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("BenchmarkRunner")

    object Opts extends ScallopConf(args) {

      var infotons = opt[Long]("infotons", short = 'i', descr = "The number of infotons to create for testing", required = true, validate = _ > 0)
      val seed = opt[Int]("seed", short = 's', descr = "A seed value for the data generation sequence", default = Some(0))

      val url = trailArg[String]("url", descr = "The URL of the CM-Well ws instance to benchmark", required = true, validate = _.nonEmpty)

      verify()
    }

    val uri = Uri(Opts.url())
    require(uri.scheme == "http", "url scheme must be HTTP.")

    // Initialize parameters used by simulation (and data generation) classes.
    SimulationParameters._infotonCount = Opts.infotons()
    SimulationParameters._baseURL = s"http://${uri.authority.host.address}:${uri.authority.port}"
    SimulationParameters._seed = Opts.seed()
    // Generate a path so that we are generating data into an empty path.
    SimulationParameters._path = s"benchmark-${System.currentTimeMillis()}"
    logger.info(s"Infotons will be generated in path ${SimulationParameters.path}")

    implicit val system: ActorSystem = ActorSystem("cmwell-benchmark")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    try {
      // Generate data and POST it to CM-Well
      CreateData(uri = uri, path = SimulationParameters.path, infotonCount = SimulationParameters.infotonCount)

      logger.info("Generation complete.")
    }
    finally {
      system.terminate()
    }
  }
}
