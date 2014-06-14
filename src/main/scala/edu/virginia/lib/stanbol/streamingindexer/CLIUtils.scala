package edu.virginia.lib.stanbol.streamingindexer

import language.postfixOps

import com.typesafe.scalalogging.slf4j.LazyLogging

object CLIUtils extends LazyLogging {

  def parseOptions(args: List[String], positional: List[Symbol], optional: Map[String, Symbol], options: Map[Symbol, String], required: Set[Symbol] = Set.empty): Map[Symbol, String] = {
    args match {

      // Empty list
      case Nil => {
        if (required forall (options.keySet contains (_))) {
          options
        } else {
          val ps = required diff (options keySet)
          logger error ("Missing required parameters: {}!", ps mkString (", "))
          sys exit (1)
        }
      }

      // Optional flags
      case key :: tail if optional isDefinedAt (key) => {
        logger debug ("Found parameter {}", key)
        parseOptions(tail, positional, optional, options ++ Map(optional(key) -> ""), required)
      }

      // Required/positional arguments
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