/**
 *
 */
package edu.virginia.lib.stanbol.streamingindexer

import collection.Iterator
import language.postfixOps

import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * A simple {@link Iterator} that chunks off sections of input (as {@link Seq}s) based on a supplied equivalence.
 *
 * @author ajs6f
 *
 */
class ChunkingIterator[T](var wrapped: Iterator[T])(equivalence: (T, T) => Boolean) extends Iterator[Seq[T]] with LazyLogging {

  override def hasNext = wrapped hasNext

  override def next = {
    val (firstPath, secondPath) = wrapped duplicate
    val head = firstPath next;
    logger trace ("Head of chunk: {}", head.asInstanceOf[AnyRef])
    val tail = firstPath takeWhile (equivalence(head, _))
    wrapped = secondPath dropWhile (equivalence(head, _))
    head +: tail.toSeq
  }
}

object ChunkingIterator {
  def chunkingIterator[T](wrapped: Iterator[T])(equivalence: (T, T) => Boolean): ChunkingIterator[T] = {
    new ChunkingIterator(wrapped)(equivalence)
  }
}