package cmwell.benchmark.simulation

import cmwell.benchmark.run.BenchmarkRunner
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef._

import scala.concurrent.duration._

class SearchWithData extends Simulation {

  private val httpProtocol = http.baseURL(BenchmarkRunner.baseURL)

  private val s = scenario("query_using_field_conditions_with-data").during(1.minutes) {

    feed(BenchmarkRunner.allFieldsFeeder)
      .exec(http("query_using_field_conditions_with-data")
        .get("/" + BenchmarkRunner.path + "?op=search&qp=myersBriggs.foaf:${myersBriggs},age.foaf:${age}&format=json&with-data")
        .check(status.is(200)))
  }

  setUp(
    s.inject(atOnceUsers(100))
  ).protocols(httpProtocol)
}