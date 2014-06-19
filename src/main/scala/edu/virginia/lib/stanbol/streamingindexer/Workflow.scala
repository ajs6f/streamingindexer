package edu.virginia.lib.stanbol.streamingindexer

import java.io.{ BufferedInputStream, File, FileInputStream, StringReader }
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.DAYS
import java.util.zip.GZIPInputStream

import collection.immutable.Map.empty
import concurrent.{ ExecutionContext, Future }
import io.Source.{ fromFile, fromInputStream }
import language.{ implicitConversions, postfixOps, reflectiveCalls }
import util.{ Failure, Success }

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer
import org.apache.solr.core.CoreContainer.createAndLoad
import org.apache.stanbol.commons.namespaceprefix.service.StanbolNamespacePrefixService
import org.apache.stanbol.entityhub.yard.solr.impl.{ SolrYard, SolrYardConfig }

import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel
import com.typesafe.scalalogging.slf4j.LazyLogging

import edu.virginia.lib.stanbol.streamingindexer.CLIUtils.parseOptions
import edu.virginia.lib.stanbol.streamingindexer.ChunkingIterator.chunkingIterator
import edu.virginia.lib.stanbol.streamingindexer.TriplesIntoRepresentation.triplesIntoRepresentation

/**
 * @author ajs6f
 */
object Workflow extends LazyLogging {

  /**
   * A predicate showing whether two lines in N-Triples begin with the same subject
   */
  val sameSubjects: (String, String) => Boolean = (a, b) =>
    a.split("\\s+")(0) equals b.split("\\s+")(0)

  implicit val threadpool = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()))

  def main(args: Array[String]): Unit = {

    val required = List('inputFile)
    val optional = Map("--yardName" -> 'yardName, "--zipped" -> 'zipped)
    val options =
      parseOptions(args toList, required, optional, empty, required toSet)

    val filename = options('inputFile)
    val yardName = options get ('yardName)
    val yard = createYard(yardName)

    doWith {
      options get ('zipped) match {
        case None => fromFile(new File(filename))
        case some => fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(filename))))
      }
    } { source =>
      chunkingIterator(source getLines)(sameSubjects) foreach { triples =>
        {
          val subject = triples(0).split("\\s+")(0)
          Future {
            logger info ("Processing RDF for: {}", subject)
            if (yard isRepresentation (subject)) {
              val rep = yard getRepresentation (subject)
              logger debug ("Retrieved extant representation: {}", rep)
              rep addTriples (triples)
              yard update (rep)
            } else {
              val rep = yard create (subject)
              logger debug ("Created new representation: {}", rep)
              rep addTriples (triples)
              yard store (rep)
            }
            logger info ("Finished indexing resource: {}", subject)
          }
        }
      }
    }
    threadpool shutdown;
    threadpool awaitTermination (3, DAYS)
    yard close;
    logger info ("Closed SolrYard")
    sys exit (0)
  }

  /**
   * Converts a sequence of strings containing triples in N3 to a {@link Model}
   *
   * @param triples
   * @return a Model of the proferred triples
   */
  implicit def triples2model(triples: Seq[String]): Model = {
    val r = new StringReader(triples mkString ("\n"))
    doWith { new StringReader(triples mkString ("\n")) } {
      createDefaultModel read (_, "", "N3")
    }

  }

  /**
   * @return a configured {@link SolrYard}
   */
  // TODO real configurability
  def createYard(yardName: Option[String]): SolrYard = {
    val solrHome = "target/classes/solr"
    val container =
      createAndLoad(solrHome, new File(solrHome, "solr.xml"))
    val config = new SolrYardConfig(yardName getOrElse ("defaultYardName"), "imaginarySolrUrl")
    config setImmediateCommit (true)
    val server = new EmbeddedSolrServer(container, "testCore")
    server commit ()
    new SolrYard(server, config, new StanbolNamespacePrefixService(null))
  }

  def doWith[T <: { def close() }, R](t: => T)(f: T => R): R = {
    try f(t) finally t close
  }

}
