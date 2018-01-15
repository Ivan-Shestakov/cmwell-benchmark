package cmwell.benchmark.run

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import cmwell.benchmark.data._
import cmwell.benchmark.monitoring._
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Random

object BenchmarkRunner extends App {


  override def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("BenchmarkRunner")

    object Opts extends ScallopConf(args) {

      var infotons = opt[Long]("infotons", descr = "The number of infotons to create for testing", required = true, validate = _ > 0)
      val output = opt[File]("output", descr = "The directory where results will be written", required = true)
      val seed = opt[Int]("seed", descr = "A seed value for the data generation sequence", default = Some(0))
      val jmxPort = opt[Int]("jmx-port", descr = "The port that JMX monitoring is exposed on bg", default = Some(7196))
      val url = trailArg[String]("url", descr = "The URL of the CM-Well ws instance to benchmark", required = true, validate = _.nonEmpty)

      // TODO: Validate that output either doesn't exist, or is an empty directory

      verify()
    }

    val uri = Uri(Opts.url())
    require(uri.scheme == "http", "url scheme must be HTTP.")

    val resultsDirectory = Opts.output().toString // TODO: Should this directory be cleared if it exists? Check it is non-empty?

    // Initialize parameters used by simulation classes.
    SimulationParameters._infotonCount = Opts.infotons()
    SimulationParameters._baseURL = s"http://${uri.authority.host.address}:${uri.authority.port}"
    SimulationParameters._seed = Opts.seed()
    // Generate a path so that we are generating data into an empty path.
    SimulationParameters._path = s"benchmark-${new Random().nextInt()}"
    logger.info(s"Infotons will be generated in path ${SimulationParameters.path}")

    implicit val system: ActorSystem = ActorSystem("cmwell-benchmark")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    try {

      // Find all the bg nodes, and monitor the persist and index flows in each of them.
      val bgHosts = BgNodes(uri.authority.host.address, uri.authority.port)

      val ingestMonitors = for {
        bgHost <- bgHosts
        (phase, metric) <- Seq(
          ("persist", "metrics:name=cmwell.bg.ImpStream.WriteCommand Counter"),
          ("index", "metrics:name=cmwell.bg.IndexerStream.IndexNewCommand Counter"))
      } yield IngestionMonitoring(
        phase = phase,
        host = bgHost,
        port = Opts.jmxPort(),
        metric = metric,
        frequency = 1.second)

      // Generate data and POST it to CM-Well
      CreateData(uri = uri, path = SimulationParameters.path, infotonCount = SimulationParameters.infotonCount)

      ingestMonitors.foreach(_.cancel())

      val ingestionRates = for (monitor <- ingestMonitors)
        yield IngestionResults(
          host = monitor.host,
          phase = monitor.phase,
          infotons = SimulationParameters.infotonCount,
          observations = monitor.result)

      writeResultsFile(resultsDirectory, "ingest", IngestionResults.toJson(ingestionRates))


      // Run the gatling simulations using that data
      val simulationRunner = new SimulationRunner(resultsDirectory)

      val simulations = Seq(
        "Get",
        "GetWithData",
        "Search",
        "SearchWithData")

      val results = for (simulation <- simulations) yield simulationRunner.run(simulation)
      writeResultsFile(resultsDirectory, "api", SimulationResults.toJson(results))

      logger.info("Benchmarking run complete.")
    }
    finally {
      system.terminate()
    }
  }

  def writeResultsFile(resultsDirectory: String, name: String, content: String): Unit = {
    new File(resultsDirectory).mkdir() // Ensure directory exists before creating a file in it.

    val file = Paths.get(resultsDirectory).resolve(s"$name.json")
    Files.write(file, content.getBytes(UTF_8))
  }
}
