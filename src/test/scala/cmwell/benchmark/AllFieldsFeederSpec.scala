package cmwell.benchmark

import cmwell.benchmark.data.AllFieldsFeeder
import org.scalatest.{FlatSpec, Matchers}

class AllFieldsFeederSpec extends FlatSpec with Matchers {

  def allFieldsFeeder = AllFieldsFeeder(seed = 0, path = s"http://example.com/path", wrapAroundAt = 5)

  "The feeder" should "contain some elements" in {

    val elements = allFieldsFeeder.take(10)

    for (e <- elements)
      println(e)
  }
}
