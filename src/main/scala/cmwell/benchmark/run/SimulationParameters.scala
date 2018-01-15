package cmwell.benchmark.run

import cmwell.benchmark.data.AllFieldsFeeder

object SimulationParameters {

  // The gatling simulation classes must be defined with zero-arg constructors,
  // so these fields are used as a way to parameterize the simulations.
  private[run] var _baseURL: String = _
  private[run] var _path: String = _
  private[run] var _seed: Int = _
  private[run] var _infotonCount: Long = _

  /** The base URL (protocol, host, port) without a trailing slash. */
  def baseURL = _baseURL

  /** The path segment (under baseURL) where the data will be generated. */
  def path = _path

  /** The random seed that defines the sequence of data that will be generated.
    * While the data is pseudo-random, the same sequence will always be generated given the same seed. */
  def seed = _seed

  /** The total number of infotons generated. */
  def infotonCount = _infotonCount

  /** A feeder that provides all the fields in the generated infoton, in the same order that the infotons are generated.
    * If more than `infotonCount` values are retrieved from this feeder, it will wrap around to the start.
    */
  def allFieldsFeeder = AllFieldsFeeder(seed = seed, path = s"$baseURL/$path", wrapAroundAt = infotonCount)

}
