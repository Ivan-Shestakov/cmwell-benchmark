package cmwell.benchmark.run

import java.io.File
import java.nio.file.{Files, Path, Paths}

import cmwell.benchmark.util.FileUtils.readFile
import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder
import org.slf4j.LoggerFactory
import spray.json.DefaultJsonProtocol._
import spray.json._

case class SimulationResult(simulation: String,
                            responseTime: Int,
                            requestsPerSecond: Double,
                            start: Long,
                            end: Long)

object SimulationResults {

  private implicit val resultFormat: RootJsonFormat[SimulationResult] = jsonFormat5(SimulationResult)

  val fileName = "api.json"

  def toJson(results: Seq[SimulationResult]): String = {
    results.toJson.prettyPrint
  }

  def fromJson(source: String): Seq[SimulationResult] = {
    source.parseJson.convertTo[Seq[SimulationResult]]
  }
}

/**
  * Runs a Gatling `Simulation` and collects the summary results.
  * The detailed results are retained on the file system.
  *
  * Gatling `Simulation`s are not designed to be launched from code, and use a zero-argument constructor.
  * A workaround is used to allow them to be parameterized (i.e., to access generated data) by referencing
  * fields of BenchmarkRunner.
  *
  * @param tempDirectory The directory that results will be placed in.
  */
class SimulationRunner(tempDirectory: File) {

  private val logger = LoggerFactory.getLogger("benchmark-runSimulation")

  private val resultsDirectory = Paths.get(tempDirectory.toString)

  // Run each simulation in this temporary directory
  private val runDirectoryName = "_run"
  private val simulationRunDirectory = resultsDirectory.resolve(runDirectoryName)

  def run(simulation: String): SimulationResult = {

    // Run this simulation
    val props = new GatlingPropertiesBuilder()
    props.simulationClass(s"cmwell.benchmark.simulation.$simulation")
    props.resultsDirectory(simulationRunDirectory.toString)

    val start = System.currentTimeMillis
    logger.info(s"Starting simulation: $simulation.")
    Gatling.fromMap(props.build)
    logger.info(s"Finished simulation $simulation.")
    val end = System.currentTimeMillis

    // In simulationRunDirectory, there will be a single directory (<simulation>-<ts>)
    val simulationDirectory: Path = Files.list(simulationRunDirectory).findFirst.get
    val resultsFile = simulationDirectory.resolve("js").resolve("global_stats.json")

    // Extract the results
    val detailedResults = readFile(resultsFile).parseJson.asJsObject

    val responseTime = detailedResults
      .fields("meanResponseTime").asJsObject
      .fields("total").convertTo[Int]

    val requestsPerSecond = detailedResults
      .fields("meanNumberOfRequestsPerSecond").asJsObject
      .fields("total").convertTo[Double]

    // Move the directory to the parent, and drop the simulation name
    Files.move(simulationDirectory, resultsDirectory.resolve(simulation))

    // Remove the run directory
    Files.delete(simulationRunDirectory)

    SimulationResult(
      simulation = simulation,
      responseTime = responseTime,
      requestsPerSecond = requestsPerSecond,
      start = start,
      end = end)
  }
}
