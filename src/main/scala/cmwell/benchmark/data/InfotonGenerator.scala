package cmwell.benchmark.data

/**
  * Creates infoton text from a template.
  * The values in each field are populated from a Feeder.
  *
  * TODO: Expand this to include more fields (esp. more data types).
  * TODO: Should generate a variable number of link fields according to some distribution function.
  *
  * @param seed         A random seed for value generation.
  * @param path         The path that the infoton will be created in.
  * @param wrapAroundAt The total number of infotons to be generated.
  */
class InfotonGenerator(seed: Int, path: String, wrapAroundAt: Long) {

  private val feeder = AllFieldsFeeder(seed, path, wrapAroundAt)

  def next(): String = {

    val x = feeder.next()
    val personId = x("personId")

    val infotonText =
      s"""<$path/${x("personId")}> <http://www.w3.org.1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
         |<$path/$personId> <http://xmlns.com/foaf/0.1/name> "${x("name")}" .
         |<$path/$personId> <http://xmlns.com/foaf/0.1/age> "${x("age")}"^^<http://www.w3.org/2001/XMLSchema#integer> .
         |<$path/$personId> <http://xmlns.com/foaf/0.1/myersBriggs> "${x("myersBriggs")}" .
         |""".stripMargin

    val previousPersonIds = x("previousPersonIds")

    // Generate a variable number of "knows" links to infotons already generated.
    // The previousPeronIds element will have zero or more personIds separated by commas.
    val previousPersonIdFields = if (previousPersonIds.isEmpty)
      ""
    else
      previousPersonIds.split(',').map(previousPersonId =>
        s""" <$path/$personId> <http://xmlns.com/foaf/0.1/knows> <$path/$previousPersonId> .""")
        .mkString

    infotonText + previousPersonIdFields
  }
}
