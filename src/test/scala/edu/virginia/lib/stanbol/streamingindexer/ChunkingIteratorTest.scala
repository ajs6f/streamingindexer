package edu.virginia.lib.stanbol.streamingindexer

import scala.util.matching.Regex

import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner._
import org.specs2.specification.Scope
import scala.util.matching.Regex

import com.typesafe.scalalogging.slf4j.LazyLogging

@RunWith(classOf[JUnitRunner])
class ChunkingIteratorTest extends Specification with LazyLogging {

  "Tests for the ChunkingIterator class".title

  isolated

  val testData = List("Bob Dave Jones", "Bob Chris Smith", "Jane Jill Doe", "Jane Sara Roe", "Jane Trish Toe")

  val testEquivalence: (String, String) => Boolean = (a, b) => {
    logger debug ("Comparing {} with {}", a, b)
    a.split("\\s+")(0) equals b.split("\\s+")(0)
  }

  val testIterator = new ChunkingIterator(testData iterator)(testEquivalence)

  "The test iterator" should {
    "have an appropriate first chunk" in {
      "have a first chunk" in {
        testIterator.hasNext mustEqual true
      }
      "have a two element first chunk" in {
        testIterator.next must have size (2)
      }
      "have a first chunk with the correct first element" in {
        val firstChunk = testIterator.next
        firstChunk.head mustEqual "Bob Dave Jones"
      }
      "have a first chunk with the correct second element" in {
        val firstChunk = testIterator.next
        firstChunk.last mustEqual "Bob Chris Smith"
      }
    }
    "have an appropriate second chunk" in {
      "have a second chunk" in {
        testIterator.next
        testIterator.hasNext mustEqual true
      }
      "have a three element second chunk" in {
        testIterator.next
        testIterator.next must have size (3)
      }
    }
    "not have a third chunk" in {
      testIterator.next
      testIterator.next
      testIterator.hasNext mustEqual false
    }
  }
}