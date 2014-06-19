package edu.virginia.lib.stanbol.streamingindexer

import collection.JavaConversions.seqAsJavaList

import org.apache.stanbol.entityhub.servicesapi.model.Representation
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel
import com.hp.hpl.jena.rdf.model.ResourceFactory.{ createLangLiteral, createPlainLiteral, createProperty, createResource }
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.impl.StatementImpl
import com.typesafe.scalalogging.slf4j.LazyLogging

import edu.virginia.lib.stanbol.streamingindexer.TriplesIntoRepresentation.triplesIntoRepresentation

@RunWith(classOf[org.specs2.runner.JUnitRunner])
class TriplesIntoRepresentationTest extends Specification with LazyLogging with Mockito {

  "Tests for the TriplesIntoRepresentation class" title

  val representation = mock[Representation]

  val subject = createResource("http://example.com/subject")
  val uriPredicate = createProperty("http://example.com/objectProperty")
  val literalPredicate = createProperty("http://example.com/literalProperty")
  val literalLangPredicate = createProperty("http://example.com/literalPropertyWithLang")
  val uriObject = createResource("http://example.com/object")
  val literal = createPlainLiteral("literal-valued object")
  val lang = "en"
  val langLiteral = createLangLiteral("literal-valued object with language tag", lang)

  val rdf: java.util.List[Statement] = List(new StatementImpl(subject, uriPredicate, uriObject),
    new StatementImpl(subject, literalPredicate, literal),
    new StatementImpl(subject, literalLangPredicate, langLiteral))
  val model = createDefaultModel add (rdf)

  "A TriplesIntoRepresentation" should {
    "create appropriate fields" in {
      representation addTriples (model)
      got {
        one(representation) addReference (uriPredicate getURI, uriObject getURI)
        one(representation) add (literalPredicate getURI, literal getLexicalForm)
        one(representation) add (literalLangPredicate getURI, langLiteral getLexicalForm)
        one(representation) addNaturalText (literalLangPredicate getURI, langLiteral getLexicalForm, lang)
      }
    }
  }
}