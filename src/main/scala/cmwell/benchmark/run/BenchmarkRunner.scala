package cmwell.benchmark.run

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import cmwell.benchmark.data._
import cmwell.benchmark.monitoring._
import org.apache.commons.io.FileUtils
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

object BenchmarkRunner extends App {

  override def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("BenchmarkRunner")

    object Opts extends ScallopConf(args) {

      var infotons = opt[Long]("infotons", descr = "The number of infotons to create for testing", required = true, validate = _ > 0)
      val seed = opt[Int]("seed", descr = "A seed value for the data generation sequence", default = Some(0))
      val jmxPort = opt[Int]("jmx-port", descr = "The port that JMX monitoring is exposed on bg", default = Some(7196))

      val rawResultsDir = opt[File]("raw-results", descr = "The directory where raw results will be written", required = false)
      val baselinesDir = opt[File]("baselines", descr = "The directory to load baselines from", required = false)
      val testResultsFile = opt[File]("junit", descr = "The file that the junit XML result is written to", required = false)

      val ingestThreshold = opt[Double]("ingest-threshold", descr = "The ingestion threshold (percentage)", default = Some(0.0))
      val simulationThreshold = opt[Double]("simulation-threshold", descr = "The simulation threshold (percentage)", default = Some(0.0))

      val url = trailArg[String]("url", descr = "The URL of the CM-Well ws instance to benchmark", required = true, validate = _.nonEmpty)

      verify()
    }

    val uri = Uri(Opts.url())
    require(uri.scheme == "http", "url scheme must be HTTP.")

    // Initialize parameters used by simulation classes.
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

      // Run the gatling simulations using that data
      val tempDirectory = makeTempDirectory()
      try {
        val simulationRunner = new SimulationRunner(tempDirectory)

        val simulations = Seq(
          "Get",
          "GetWithData",
          "Search",
          "SearchWithData")

        val simulationResults = for (simulation <- simulations) yield simulationRunner.run(simulation)

        ReportResults(
          baselinesDir = Opts.baselinesDir.toOption,
          rawResultsDir = Opts.rawResultsDir.toOption,
          testResultsFile = Opts.testResultsFile.toOption,
          ingestionResults = ingestionRates,
          simulationResults = simulationResults,
          ingestThreshold = Opts.ingestThreshold(),
          simulationThreshold = Opts.simulationThreshold())
      }
      finally {
        cleanTempDirectory(tempDirectory)
      }

      logger.info("Benchmarking run complete.")
    }
    finally {
      system.terminate()
    }
  }

  private def makeTempDirectory(): File = {
    val name = "cmwell-benchmark-" + System.currentTimeMillis
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), name)
    tmpDir.mkdir()

    tmpDir
  }

  private def cleanTempDirectory(tempDirectory: File): Unit = {
    FileUtils.deleteDirectory(tempDirectory)
  }
}
