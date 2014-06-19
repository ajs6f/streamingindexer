package edu.virginia.lib.stanbol.streamingindexer

import collection.JavaConversions.asScalaIterator
import language.{ implicitConversions, postfixOps }

import org.apache.stanbol.entityhub.servicesapi.model.Representation

import com.hp.hpl.jena.rdf.model.Model
import com.typesafe.scalalogging.slf4j.LazyLogging

class TriplesIntoRepresentation(val rep: Representation) extends LazyLogging {
  /**
   * Adds RDF to a {@link Representation}
   *
   * @param triples the RDF to add
   */
  def addTriples(triples: Model) = {
    triples.listStatements.foreach(
      t => {
        logger debug ("Processing triple: {}", t)
        val (s, p, o) = (t getSubject, t getPredicate, t getObject)
        logger trace ("Found subject: {}", s)
        val pred = p getURI;
        logger trace ("Found predicate: {}", pred)
        if (o isLiteral) {
          logger trace ("Found a literal object.")
          val literal = o asLiteral
          val (lexical, lang) = (literal getLexicalForm, literal getLanguage)
          logger trace ("Found lexical form: {}", lexical)
          logger trace ("Found language: {}", lang)
          if (lang nonEmpty) {
            rep addNaturalText (pred, lexical, lang)
          }
          rep add (pred, lexical)
        } else {
          if (o isURIResource) {
            rep addReference (pred, o.asResource().getURI)
          }
        }
      })
  }
}

object TriplesIntoRepresentation {
  implicit def triplesIntoRepresentation(rep: Representation): TriplesIntoRepresentation = {
    new TriplesIntoRepresentation(rep)
  }
}