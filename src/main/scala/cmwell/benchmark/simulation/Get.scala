package cmwell.benchmark.simulation

import cmwell.benchmark.run.BenchmarkRunner
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef._

import scala.concurrent.duration._

class Get extends Simulation {

  private val httpProtocol = http.baseURL(BenchmarkRunner.baseURL)

  private val s = scenario("get_single_infoton_by_URI").during(1.minutes) {

    feed(BenchmarkRunner.allFieldsFeeder)
      .exec(http("get_single_infoton_by_URI")
        .get("/" + BenchmarkRunner.path + "/${personId}?format=json")
        .check(status.is(200)))
  }

  setUp(
    s.inject(atOnceUsers(100))
  ).protocols(httpProtocol)
}