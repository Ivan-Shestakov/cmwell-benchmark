package cmwell.benchmark.run

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import cmwell.benchmark.monitoring.{IngestionResult, IngestionResults}
import org.apache.commons.io.FileUtils

import scala.xml.NodeSeq

/**
  * Convert the results to JUnit format and write to file.
  * Also writes the results to file in case they are needed to create a baseline.
  */
object ReportResults {

  private val xunitResultsFileName = "benchmark-results.xml"

  def apply(baselinesDir: Option[File],
            rawResultsDir: Option[File],
            testResultsFile: Option[File],
            ingestionResults: Seq[IngestionResult],
            simulationResults: Seq[SimulationResult],
            ingestThreshold: Double,
            simulationThreshold: Double): Unit = {

    // Write out the raw results if a output directory was provided.
    if (rawResultsDir.isDefined) {
      writeResultsFile(rawResultsDir.get, IngestionResults.fileName, IngestionResults.toJson(ingestionResults))
      writeResultsFile(rawResultsDir.get, SimulationResults.fileName, SimulationResults.toJson(simulationResults))
    }

    // load baselines
    val (ingestBaselines, simulationBaselines) = loadBaselines(baselinesDir)

    val ingest = zipIngestBaselines(ingestBaselines, ingestionResults)
    val simulation = zipSimulationBaselines(simulationBaselines, simulationResults)

    val timestamp = formatTimestamp(System.currentTimeMillis)

    val simulationTestCases = simulation.map { case (baseline, current) =>

      val name = if (baseline.isDefined) baseline.get.simulation else current.get.simulation
      val stdout = current.fold("")(x =>
        s"Requests/second: ${x.requestsPerSecond}\nMean response time (ms): ${x.responseTime}")

      val stderr = Seq[Option[String]](
        current.fold[Option[String]](Some("This simulation was not measured in this run."))(_ => None),
        baseline.fold[Option[String]](Some("There was no baseline provided for this simulation."))(_ => None))
        .flatten.mkString("\n")

      val time = (current.get.end - current.get.start) / 1000.0

      <testcase name={name} classname={"cmwell.benchmark.simulation." + name} time={time.toString}>
        {simulationError(baseline, current, simulationThreshold)}<system-out>
        {stdout}
      </system-out>
        <system-err>
          {stderr}
        </system-err>
      </testcase>
    }

    val ingestionTestCases = ingest.map { case (baseline, current) =>

      def makeName(x: IngestionResult) = s"${x.phase} - ${x.host}"

      val name = makeName(if (baseline.isDefined) baseline.get else current.get)
      val stdout = current.fold("")(x => s"IngestionRate: ${x.rate} infotons/second)")

      val stderr = Seq[Option[String]](
        current.fold[Option[String]](Some("This ingestion phase was not measured in this run."))(_ => None),
        baseline.fold[Option[String]](Some("There was no baseline provided for this ingestion phase."))(_ => None))
        .flatten.mkString("\n")

      val time = (current.get.end - current.get.start) / 1000.0

      <testcase name={name} classname={"cmwell.benchmark.simulation." + name} time={time.toString}>
        {ingestError(baseline, current, ingestThreshold)}<system-out>
        {stdout}
      </system-out>
        <system-err>
          {stderr}
        </system-err>
      </testcase>
    }

    val junitResults =
      <testsuites>

        <testsuite name="simulation" package="cmwell.benchmark" skipped="0" tests={simulation.length.toString} timestamp={timestamp}>
          <properties>
            <property name="threshold" value={simulationThreshold.toString}/>
          </properties>{simulationTestCases}
        </testsuite>

        <testsuite name="ingestion" package="cmwell.benchmark" skipped="" tests={ingest.length.toString} timestamp={timestamp}>
          <properties>
            <property name="threshold" value={ingestThreshold.toString}/>
            <property name="infotons" value={ingestionResults.head.infotons.toString}/>
          </properties>{ingestionTestCases}
        </testsuite>

      </testsuites>

    if (testResultsFile.isDefined) {
      writeResultsFile(testResultsFile.get, xunitResultsFileName, junitResults.toString)
    }
  }

  private def ingestError(baseline: Option[IngestionResult],
                          current: Option[IngestionResult],
                          threshold: Double): NodeSeq = {
    if (baseline.isDefined && current.isDefined) {
      val baselineRate = baseline.get.rate
      val currentRate = current.get.rate

      val rateDelta = ((baselineRate - currentRate) / baselineRate) * 100
      val errorMessage = if (Math.abs(rateDelta) > threshold)
        s"The ingestion rate of ${format2(currentRate)} has changed by ${format2(rateDelta)}% as compared with the baseline of ${format2(baselineRate)}."
      else
        ""

      if (errorMessage.nonEmpty)
          <failure message={errorMessage}/>
      else
        NodeSeq.Empty
    }
    else {
      NodeSeq.Empty
    }
  }

  private def simulationError(baseline: Option[SimulationResult],
                              current: Option[SimulationResult],
                              threshold: Double): NodeSeq = {

    if (baseline.isDefined && current.isDefined) {
      val baselineResponseTime = baseline.get.responseTime.toDouble
      val currentResponseTime = current.get.responseTime.toDouble
      val baselineRequestsPerSecond = baseline.get.requestsPerSecond
      val currentRequestsPerSecond = current.get.requestsPerSecond

      val responseTimeDelta = ((baselineResponseTime - currentResponseTime) / baselineResponseTime) * 100
      val requestsPerSecondDelta = ((baselineRequestsPerSecond - currentRequestsPerSecond) / baselineRequestsPerSecond) * 100

      val errorMessage = if (Math.abs(responseTimeDelta) > threshold)
        s"The response time of ${currentResponseTime.toLong} has changed more than ${format2(threshold)}% relative to the baseline of ${format2(baselineResponseTime)}."
      else if (Math.abs(requestsPerSecondDelta) > threshold)
        s"The requests/second of ${format2(currentRequestsPerSecond)} has changed more than $threshold% relative to the baseline of ${format2(baselineResponseTime)}."
      else
        ""

      if (errorMessage.nonEmpty)
          <failure message={errorMessage}/>
      else
        NodeSeq.Empty
    }
    else {
      NodeSeq.Empty
    }
  }

  private def zipIngestBaselines(baseline: Seq[IngestionResult],
                                 current: Seq[IngestionResult]
                                ): Seq[(Option[IngestionResult], Option[IngestionResult])] = {

    // Ingestion results are matched on (phase, host)
    val phases = Set(baseline.map(_.phase) ++ current.map(_.phase): _*).toSeq.sorted
    val hosts = Set(baseline.map(_.host) ++ current.map(_.host): _*).toSeq.sorted

    for {
      phase <- phases
      host <- hosts
    } yield (baseline.find(x => x.phase == phase && x.host == host), current.find(x => x.phase == phase && x.host == host))
  }

  private def zipSimulationBaselines(baseline: Seq[SimulationResult],
                                     current: Seq[SimulationResult]
                                    ): Seq[(Option[SimulationResult], Option[SimulationResult])] = {

    // Simulation results are matched on simulation
    val simulations = Set(baseline.map(_.simulation) ++ current.map(_.simulation): _*).toSeq.sorted

    for (s <- simulations)
      yield (baseline.find(x => x.simulation == s), current.find(x => x.simulation == s))
  }

  private def loadBaselines(maybeDir: Option[File]): (Seq[IngestionResult], Seq[SimulationResult]) = {
    if (maybeDir.isDefined) {

      val ingestFile = new File(maybeDir.get, IngestionResults.fileName)
      val ingestBaselines = if (ingestFile.exists)
        IngestionResults.fromJson(FileUtils.readFileToString(ingestFile, UTF_8))
      else
        Seq.empty[IngestionResult]

      val simulationFile = new File(maybeDir.get, SimulationResults.fileName)
      val simulationBaselines = if (simulationFile.exists)
        SimulationResults.fromJson(FileUtils.readFileToString(simulationFile, UTF_8))
      else
        Seq.empty[SimulationResult]

      (ingestBaselines, simulationBaselines)
    }
    else {
      (Seq.empty[IngestionResult], Seq.empty[SimulationResult])
    }
  }

  private def format2(x: Double) = "%1.2f".format(x) // round to 2 decimal places

  private def writeResultsFile(rawResultsDirectory: File, name: String, content: String): Unit = {
    rawResultsDirectory.mkdir() // Ensure directory exists before creating a file in it.

    val file = Paths.get(rawResultsDirectory.toString).resolve(name)
    Files.write(file, content.getBytes(UTF_8))
  }

  private def formatTimestamp(ts: Long): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    format.setTimeZone(TimeZone.getTimeZone("UTC"))
    format.format(new Date(ts))
  }
}
