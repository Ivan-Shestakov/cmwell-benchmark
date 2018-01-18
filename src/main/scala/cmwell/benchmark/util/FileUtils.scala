package cmwell.benchmark.util

import scala.io.Source

object FileUtils {

  def readResourceLines(name: String): Seq[String] = {
    val stream = getClass.getResourceAsStream(name)

    try
      Source.fromInputStream(stream).getLines.toVector
    finally
      stream.close()
  }
}
