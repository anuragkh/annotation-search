package edu.berkeley.cs.succinct.annot

import edu.berkeley.cs.succinct.buffers.annot.Annotation

/**
  * A logical operator on annotated documents.
  */
abstract class Operator

/**
  * Search operator: finds all (documentId, startOffset, endOffset) triplets where the input query
  * string occurs.
  *
  * @param query The query string.
  */
case class Search(query: String) extends Operator

/**
  * Regex operator: finds all (documentId, startOffset, endOffset) triplets where the input regex
  * pattern occurs.
  *
  * @param query The regex pattern.
  */
case class Regex(query: String) extends Operator

/**
  * FilterAnnotation operator: Filters all annotations that match regex filters corresponding to
  * annotation class and type, and an arbitrary metadataFilter function on annotation metadata.
  *
  * @param annotClassFilter Regex metadataFilter on annotation class.
  * @param annotTypeFilter  Regex metadataFilter on annotation type.
  * @param metadataFilter   Arbitrary metadataFilter function to apply on annotation metadata.
  * @param textFilter       Arbitrary metadataFilter function to apply on document text corresponding to the
  *                         annotation.
  */
case class FilterAnnotations(annotClassFilter: String, annotTypeFilter: String = ".*",
                             metadataFilter: String => Boolean = null,
                             textFilter: String => Boolean = null) extends Operator

/**
  * Contains operator: Binary operator that finds all results in its first operand that contain
  * results in the second operand.
  *
  * @param A First operand.
  * @param B Second operand.
  */
case class Contains(A: Operator, B: Operator) extends Operator

/**
  * ContainedIn operator: Binary operator that finds all results in its first operand that are
  * contained in results in the second operand.
  *
  * @param A First operand.
  * @param B Second operand.
  */
case class ContainedIn(A: Operator, B: Operator) extends Operator

/**
  * Contains operator: Binary operator that finds all results in its first operand that occur before
  * results in the second operand.
  *
  * @param A First operand.
  * @param B Second operand.
  */
case class Before(A: Operator, B: Operator, distance: Int = -1) extends Operator

/**
  * Contains operator: Binary operator that finds all results in its first operand that occur after
  * results in the second operand.
  *
  * @param A First operand.
  * @param B Second operand.
  */
case class After(A: Operator, B: Operator, distance: Int = -1) extends Operator

/**
  * A container for a single result entry.
  *
  * @param docId       The document ID.
  * @param startOffset The start offset.
  * @param endOffset   The end offset.
  * @param annotation  Associated annotation, if any.
  */
case class Result(docId: String, startOffset: Int, endOffset: Int, annotation: Annotation) {

  def canEqual(a: Any) = a.isInstanceOf[Result]

  override def equals(that: Any): Boolean = {
    that match {
      case o: Result => {
        o.canEqual(this) &&
          (docId.equals(o.docId) && startOffset.equals(o.startOffset) && endOffset.equals(o.endOffset))
      }
      case _ => false
    }
  }

  override def hashCode(): Int = docId.hashCode + startOffset + endOffset
}
