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

  // The gatling simulation classes must be defined with zero-arg constructors,
  // so these fields are used as a way to parameterize the simulations.
  private var _baseURL: String = _
  private var _path: String = _
  private var _seed: Int = _
  private var _infotonCount: Long = _

  /** The base URL (protocol, host, port) without a trailing slash. */
  def baseURL = _baseURL

  /** The path segment (under baseURL) where the data will be generated. */
  def path = _path

  /** The random seed that defines the sequence of data that will be generated.
    * While the data is pseudo-random, the same sequence will always be generated given the same seed. */
  def seed = _seed

  /** The total number of infotons generated. */
  def infotonCount = _infotonCount

  /** A feeder that provides all the fields in the generated infoton, in the same order that the infotons are generated.
    * If more than `infotonCount` values are retrieved from this feeder, it will wrap around to the start.
    */
  def allFieldsFeeder = AllFieldsFeeder(seed = seed, path = s"$baseURL/$path", wrapAroundAt = infotonCount)


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

    // Initialize fields used by simulation classes.
    _infotonCount = Opts.infotons()
    _baseURL = s"http://${uri.authority.host.address}:${uri.authority.port}"
    _seed = Opts.seed()

    val resultsDirectory = Opts.output().toString // TODO: Should this directory be cleared if it exists? Check it is non-empty?

    // Generate a path so that we are generating data into an empty path.
    _path = s"benchmark-${new Random().nextInt()}"
    logger.info(s"Infotons will be generated in path $path")

    implicit val system: ActorSystem = ActorSystem("cmwell-benchmark")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    // Find all the bg nodes, and monitor them all
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
    CreateData(uri = uri, path = path, infotonCount = infotonCount)

    ingestMonitors.forall(_.cancel())

    val ingestionRates = for {
      monitor <- ingestMonitors
      observations = Await.result(monitor.result, Duration.Inf)
    } yield IngestionResults(host = monitor.host, phase = monitor.phase, infotons = infotonCount, observations)

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

  def writeResultsFile(resultsDirectory: String, name: String, content: String): Unit = {
    new File(resultsDirectory).mkdir()  // Ensure directory exists before creating a file in it.

    val file = Paths.get(resultsDirectory).resolve(s"$name.json")
    Files.write(file, content.getBytes(UTF_8))
  }
}
