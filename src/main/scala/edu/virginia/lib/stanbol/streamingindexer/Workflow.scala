package edu.virginia.lib.stanbol.streamingindexer

import java.io.{BufferedInputStream, File, FileInputStream, StringReader}
import java.lang.Runtime.getRuntime
import java.util.zip.GZIPInputStream

import actors.threadpool.Executors.newFixedThreadPool
import actors.threadpool.TimeUnit.DAYS
import collection.immutable.Map.empty
import io.Source.{fromFile, fromInputStream}
import language.{implicitConversions, postfixOps}
import util.{Failure, Success, Try}

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer
import org.apache.solr.core.CoreContainer.createAndLoad
import org.apache.stanbol.commons.namespaceprefix.service.StanbolNamespacePrefixService
import org.apache.stanbol.entityhub.yard.solr.impl.{SolrYard, SolrYardConfig}

import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel
import com.hp.hpl.jena.rdf.model.ResourceFactory.createResource
import com.typesafe.scalalogging.slf4j.LazyLogging

import edu.virginia.lib.stanbol.streamingindexer.CLIUtils.parseOptions
import edu.virginia.lib.stanbol.streamingindexer.ChunkingIterator.chunkingIterator
import edu.virginia.lib.stanbol.streamingindexer.TriplesIntoRepresentation.triplesIntoRepresentation

/**
 * @author ajs6f
 */
object Workflow extends LazyLogging {

  val defaultYardName = "testYard"

  var yard: SolrYard = null

  val threadpool = newFixedThreadPool(getRuntime availableProcessors)

  /**
   * A predicate showing whether two lines in N-Triples begin with the same subject
   */
  val sameSubjects: (String, String) => Boolean = (a, b) =>
    a.split("\\s+")(0) equals b.split("\\s+")(0)

  def main(args: Array[String]): Unit = {

    val required = List('inputFile)
    val optional = Map("--yardName" -> 'yardName, "--zipped" -> 'zipped)
    val options = parseOptions(args toList, required, optional, empty[Symbol, String])

    yard = createYard(options get ('yardName))

    val source = options get ('inputFile) match {
      case None => {
        logger error ("Input filename required!")
        sys exit (1)
      }
      case Some(filename) => options get ('zipped) match {
        case None => fromFile(new File(filename))
        case some => fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(filename))))
      }
    }

    val rdf = chunkingIterator(source getLines)(sameSubjects)
    rdf foreach { triples =>
      threadpool submit (
        new Runnable {
          override def run {
            val subject = triples(0).split("\\s+")(0)
            Try(createResource(subject)) match {
              case Success(s) => {
                logger info ("Processing RDF for: {}", s)
                processEntity(subject, triples)
              }
              case Failure(e) => logger error ("Failed to parse RDF subject: {}!\n{}", subject, e)
            }
          }
        })
    }
    threadpool.shutdown
    threadpool awaitTermination (3, DAYS)
    yard.close
    sys exit (0)
  }

  def processEntity(subject: String, triples: Seq[String]) {
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
  }

  /**
   * Converts a sequence of strings containing triples in N3 to a {@link Model}
   *
   * @param triples
   * @return
   */
  implicit def triples2model(triples: Seq[String]): Model = {
    val r = new StringReader(triples mkString ("\n"))
    try { createDefaultModel read (r, "", "N3") }
    finally { r close }
  }

  /**
   * @return a configured {@link SolrYard}
   */
  // TODO actual configuration
  def createYard(yardName: Option[String]): SolrYard = {
    val solrHome = "target/classes/solr"
    val container =
      createAndLoad(solrHome, new File(solrHome, "solr.xml"))
    val config = yardName match {
      case None => new SolrYardConfig(defaultYardName, "imaginarySolrUrl")
      case Some(y) =>
        new SolrYardConfig(y, "imaginarySolrUrl")
    }
    config setImmediateCommit (true)
    val server = new EmbeddedSolrServer(container, "testCore")
    server commit ()
    new SolrYard(server, config, new StanbolNamespacePrefixService(null))
  }

}
