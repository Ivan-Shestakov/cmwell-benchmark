package cmwell.benchmark.simulation

import cmwell.benchmark.run.SimulationParameters
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef._

import scala.concurrent.duration._

class SearchWithData extends Simulation {

  private val httpProtocol = http.baseURL(SimulationParameters.baseURL)

  private val s = scenario("query_using_field_conditions_with-data").during(1.minutes) {

    feed(SimulationParameters.allFieldsFeeder)
      .exec(http("query_using_field_conditions_with-data")
        .get("/" + SimulationParameters.path + "?op=search&qp=myersBriggs.foaf:${myersBriggs},age.foaf:${age}&format=json&with-data")
        .check(status.is(200)))
  }

  setUp(
    s.inject(atOnceUsers(100))
  ).protocols(httpProtocol)
}