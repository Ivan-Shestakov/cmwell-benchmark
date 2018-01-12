package cmwell.benchmark.data

import cmwell.benchmark.util.FileUtils.readResourceLines
import cmwell.benchmark.util.MathUtils.nonNegativeMod
import io.gatling.core.feeder.Feeder

import scala.util.Random


/** A Feeder that produces all the field values in the infoton */
object AllFieldsFeeder {

  private val allMyersBriggs = readResourceLines("/cmwell/benchmark/myersBriggs.txt")
  private val allNames = readResourceLines("/cmwell/benchmark/names.txt")

  private def fromResourceValues(x: Int, values: Seq[String]): String = values(nonNegativeMod(x, values.length))

  def apply(seed: Int = 0,
            path: String,
            wrapAroundAt: Long): Feeder[String] = {

    var randomGenerator: Random = null
    var previousRandomValue = 0
    var position = 0L

    Iterator.continually {

      // Wrap around so that if we are sampling this iterator (e.g., to provide query parameters),
      // if we reach `wrapAroundAt` (which is presumably the number of values generated), then we can
      // keep producing values from the sequence of wrapAroundAt values that were generated.
      if (position % wrapAroundAt == 0) {
        position = 0
        randomGenerator = new Random(seed)
        previousRandomValue = randomGenerator.nextInt()
      }

      val currentRandom = randomGenerator.nextInt()

      val r = Map(
        "personId" -> currentRandom.toString,
        "name" -> fromResourceValues(currentRandom, allNames),
        "myersBriggs" -> fromResourceValues(currentRandom, allMyersBriggs),
        "age" -> nonNegativeMod(currentRandom, 100).toString,
        "previousPersonId" -> previousRandomValue.toString
      )

      previousRandomValue = currentRandom
      position += 1

      r
    }
  }
}
