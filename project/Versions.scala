import java.io.PrintWriter

import scala.io.Source
object Versions {

  def replaceVersionInReadmeForSbt(currentVersion: String): Unit = {
    val pattern = """val fdbPubSubVersion = "([^\"]*)""""
    val newVersion = s"""val fdbPubSubVersion = "$currentVersion""""
    replaceVersionInReadme(pattern, newVersion)
  }

  def replaceVersionInReadmeForMaven(currentVersion: String): Unit = {
    val pattern = """<version>([^\"]*)</version>"""
    val newVersion = s"""  <version>$currentVersion</version>"""
    replaceVersionInReadme(pattern, newVersion)
  }

  private def replaceVersionInReadme(patternToFind: String, replaceWith: String): Unit = {
    val source = Source.fromFile("README.md")
    val updatedReadme = source
      .getLines()
      .map { line =>
        if (line.trim().matches(patternToFind)) replaceWith
        else line
      }
      .mkString("\n") + "\n"
    source.close()
    new PrintWriter("README.md") { write(updatedReadme); close() }
  }

}
