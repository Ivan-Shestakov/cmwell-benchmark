package cmwell.benchmark.util

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path

import scala.io.Source

object FileUtils {

  def readResourceLines(name: String): Seq[String] = {
    val stream = getClass.getResourceAsStream(name)

    try
      Source.fromInputStream(stream).getLines.toVector
    finally
      stream.close()
  }

  def readFile(path: Path): String = {
    val reader = Source.fromFile(path.toFile)(UTF_8)

    try reader.getLines.mkString("\n")
    finally reader.close()
  }
}
