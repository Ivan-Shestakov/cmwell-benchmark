package cmwell.benchmark.simulation

import cmwell.benchmark.run.SimulationParameters
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef._

import scala.concurrent.duration._

class Get extends Simulation {

  private val httpProtocol = http.baseURL(SimulationParameters.baseURL)

  private val s = scenario("get_single_infoton_by_URI").during(1.minutes) {

    feed(SimulationParameters.allFieldsFeeder)
      .exec(http("get_single_infoton_by_URI")
        .get("/" + SimulationParameters.path + "/${personId}?format=json")
        .check(status.is(200)))
  }

  setUp(
    s.inject(atOnceUsers(100))
  ).protocols(httpProtocol)
}