package cmwell.benchmark

import scala.io.Source
import scala.util.Random

class Generator(seed: Int, path: String) {

  private val random = new Random(seed)

  private var personId = random.nextInt

  private val allMyersBriggs = Source.fromResource(s"cmwell/benchmark/myersBriggs.txt").getLines.toVector
  private val allNames = Source.fromResource(s"cmwell/benchmark/names.txt").getLines.toVector

  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  private def fromList(x: Int, values: Seq[String]): String = values(nonNegativeMod(x, values.length))

  def next(): String = {

    val previousPersonId = personId
    personId = random.nextInt

    val myersBriggs = fromList(personId, allMyersBriggs)
    val name = fromList(personId, allNames)
    val age = nonNegativeMod(personId, 100)

    s"""<$path/$personId> <http://www.w3.org.1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
       |<$path/$personId> <http://xmlns.com/foaf/0.1/name> "$name" .
       |<$path/$personId> <http://xmlns.com/foaf/0.1/age> "$age"^^<http://www.w3.org/2001/XMLSchema#integer> .
       |<$path/$personId> <http://xmlns.com/foaf/0.1/myersBriggs> "$myersBriggs" .
       |<$path/$personId> <http://xmlns.com/foaf/0.1/knows> <$path/$previousPersonId> .
       |""".stripMargin
  }
}
