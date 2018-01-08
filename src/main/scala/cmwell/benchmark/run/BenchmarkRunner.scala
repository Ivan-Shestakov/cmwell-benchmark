package cmwell.benchmark.run

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import akka.http.scaladsl.model.Uri
import cmwell.benchmark.data._
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.Random

object BenchmarkRunner extends App {

  // The gatling simulation classes must be defined with zero-arg constructors,
  // so these fields are used as a way to parameterize the simulations.
  private var _baseURL: String = _
  private var _path: String = _
  private var _seed: Int = _
  private var _infotonCount: Int = _

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
      val url = trailArg[String]("url", descr = "The URL of the CM-Well server instance to benchmark", required = true, validate = _.nonEmpty)

      // TODO: Validate that output either doesn't exist, or is an empty directory

      verify()
    }

    val uri = Uri(Opts.url())
    require(uri.scheme == "http", "URL scheme must be HTTP (configuring connection pool for https is not implemented yet).")

    // Initialize fields used by simulation classes.
    _baseURL = s"http://${uri.authority.host.address}:${uri.authority.port}"
    _seed = Opts.seed()

    val resultsDirectory = Opts.output().toString // TODO: Should this directory be cleared if it exists? Check it is non-empty?

    val infotonsPerChunk = 1000 // POST this many infotons per request.
    val chunks = Math.ceil(Opts.infotons().toDouble / infotonsPerChunk).toInt
    val actualInfotonCount = chunks * infotonsPerChunk
    _infotonCount = actualInfotonCount

    // Generate a path so that we are generating data into an empty path.
    _path = s"benchmark-${new Random().nextInt()}"
    logger.info(s"Infotons will be generated in path $path")


    // Generate data and POST it to CM-Well
    CreateData(uri = uri, path = path, chunks = chunks, infotonsPerChunk = infotonsPerChunk)

    // Run the gatling simulations using that data
    val simulationRunner = new SimulationRunner(resultsDirectory)

    val simulations = Seq(
      "Get",
      "GetWithData",
      "Search",
      "SearchWithData")

    val results = for (simulation <- simulations) yield simulationRunner.run(simulation)

    // Convert the results to JSON and save it to file
    implicit val resultFormat: RootJsonFormat[SimulationResult] = jsonFormat3(SimulationResult)
    val summaryResults = results.toJson.prettyPrint

    val summaryFile = Paths.get(resultsDirectory).resolve("benchmark-summary.json")
    Files.write(summaryFile, summaryResults.getBytes(UTF_8))
    logger.info(s"Summary results (written to $summaryFile):\n$summaryResults")

    // TODO: Clean up the created data
  }
}
