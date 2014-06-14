package edu.virginia.lib.stanbol.streamingindexer

import language.postfixOps

import Set.empty
import com.typesafe.scalalogging.slf4j.LazyLogging

object CLIUtils extends LazyLogging {

  def parseOptions(args: List[String], positional: List[Symbol],
    optional: Map[String, Symbol], options: Map[Symbol, String],
    required: Set[Symbol] = empty): Map[Symbol, String] = {
    args match {

      // Empty list
      case Nil => {
        val ps = required diff (options keySet)
        if (ps isEmpty) options
        else {
          logger error ("Missing required parameters: {}!", ps map (_ name) mkString (", "))
          sys exit (1)
        }
      }

      // Flags
      case flag :: tail if optional isDefinedAt (flag) => {
        logger debug ("Found flag {}", flag)
        parseOptions(tail, positional, optional, options ++ Map(optional(flag) -> ""), required)
      }

      // Positional arguments
      case value :: tail if positional nonEmpty => {
        logger debug ("Found positional argument {} with value {}", positional head, value)
        parseOptions(tail, positional tail, optional, options ++ Map(positional.head -> value), required)
      }

      // Fail if an unknown argument is received
      case _ =>
        logger error ("Unknown argument(s): {}\n", args mkString (", "))
        sys exit (1)
    }
  }
}