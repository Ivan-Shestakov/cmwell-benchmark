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

  private val personIdChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

  def apply(seed: Int = 0,
            path: String,
            wrapAroundAt: Long): Feeder[String] = {

    var randomGenerator: Random = null
    var position = 0L

    // Maintain a resevoir of personIds that we have seen so far.
    val previousPersonIdResevoir = Array.ofDim[String](1024)
    var currentPreviousPersonId = 0

    // For each individual, generate between 0 and maxPreviousPersonIdLinks knows links.
    val maxPreviousPersonIdLinks = 10

    Iterator.continually {

      // Wrap around so that if we are sampling this iterator (e.g., to provide query parameters),
      // if we reach `wrapAroundAt` (which is presumably the number of values generated), then we can
      // keep producing values from the sequence of wrapAroundAt values that were generated.
      if (position % wrapAroundAt == 0) {
        position = 0
        randomGenerator = new Random(seed)

        for (i <- previousPersonIdResevoir.indices) previousPersonIdResevoir(i) = null
        currentPreviousPersonId = 0
      }

      val currentRandom = randomGenerator.nextInt()
      val personId = Seq.fill(16)(personIdChars(randomGenerator.nextInt(personIdChars.length))).mkString

      // Generate up to maxPreviousPersonId personIds to personIds in the resevoir.
      // There may be less if the resevoir is not filled or if the same element is selected twice.
      // In most cases, the count should be something close to maxPreviousPersonId.
      val previousPersonIdCount = currentPreviousPersonId min nonNegativeMod(currentRandom, maxPreviousPersonIdLinks + 1)

      // Since the feeder must have a single element type, generate a string separated by commas.
      val previousPersonIds = (0 until previousPersonIdCount)
        .map(_ => previousPersonIdResevoir(nonNegativeMod(randomGenerator.nextInt(), currentPreviousPersonId)))
        .mkString(",")

      val r = Map(
        "personId" -> personId,
        "name" -> fromResourceValues(currentRandom, allNames),
        "myersBriggs" -> fromResourceValues(currentRandom, allMyersBriggs),
        "age" -> nonNegativeMod(currentRandom, 100).toString,
        "previousPersonIds" -> previousPersonIds
      )

      if (currentPreviousPersonId < previousPersonIdResevoir.length) {
        // Resevoir not filled up yet, so just append this one.
        previousPersonIdResevoir(currentPreviousPersonId) = personId
        currentPreviousPersonId += 1
      }
      else {
        // Replace some random personId in the resevoir
        previousPersonIdResevoir(nonNegativeMod(currentRandom, previousPersonIdResevoir.length)) = personId
      }

      position += 1

      r
    }
  }
}
