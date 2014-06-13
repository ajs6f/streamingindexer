package edu.virginia.lib.stanbol.streamingindexer

import com.typesafe.scalalogging.slf4j.LazyLogging

object CLIUtils extends LazyLogging {

  def parseOptions(args: List[String], required: List[Symbol], optional: Map[String, Symbol], options: Map[Symbol, String]): Map[Symbol, String] = {
    args match {

      // Empty list
      case Nil => options

      // Optional flags
      case key :: tail if optional isDefinedAt (key) => {
        logger debug ("Found parameter {}", key)
        parseOptions(tail, required, optional, options ++ Map(optional(key) -> ""))
      }

      // Required/positional arguments
      case value :: tail if required nonEmpty => {
        logger debug ("Found positional argument {} with value", required.head, value)
        parseOptions(tail, required tail, optional, options ++ Map(required.head -> value))
      }

      // Fail if an unknown argument is received
      case _ =>
        logger error ("Unknown argument(s): {}\n", args mkString (", "))
        sys exit (1)
    }
  }
}