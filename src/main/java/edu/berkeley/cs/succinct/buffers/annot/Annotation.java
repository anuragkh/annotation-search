package edu.berkeley.cs.succinct.buffers.annot;

import java.io.Serializable;

public class Annotation implements Serializable {
  private String annotClass;
  private String annotType;
  private String docId;
  private int id;
  private int startOffset;
  private int endOffset;
  private String metadata;

  public Annotation(String annotClass, String annotType, String docId) {
    this.annotClass = annotClass;
    this.annotType = annotType;
    this.docId = docId;
  }

  public Annotation(String annotClass, String annotType, String docId, int id, int rBegin, int rEnd,
    String metadata) {
    this.annotClass = annotClass;
    this.annotType = annotType;
    this.docId = docId;
    this.id = id;
    this.startOffset = rBegin;
    this.endOffset = rEnd;
    this.metadata = metadata;
  }

  /**
   * Set the annotation ID.
   *
   * @param id The annotation ID.
   */
  public void setId(int id) {
    this.id = id;
  }

  /**
   * Set the start offset.
   *
   * @param startOffset The start offset.
   */
  public void setStartOffset(int startOffset) {
    this.startOffset = startOffset;
  }

  /**
   * Set the end offset.
   *
   * @param endOffset The end offset.
   */
  public void setEndOffset(int endOffset) {
    this.endOffset = endOffset;
  }

  /**
   * Set the metadata.
   *
   * @param metadata The metadata.
   */
  public void setMetadata(String metadata) {
    this.metadata = metadata;
  }

  /**
   * Get the Annotation Class.
   *
   * @return The Annotation Class.
   */
  public String getAnnotClass() {
    return annotClass;
  }

  /**
   * Get the Annotation Type.
   *
   * @return The annotation Type.
   */
  public String getAnnotType() {
    return annotType;
  }

  /**
   * Get the document ID for the annotation.
   *
   * @return The document ID for the annotation.
   */
  public String getDocId() {
    return docId;
  }

  /**
   * Get the annotation ID for the annotation.
   *
   * @return The annotation ID for the annotation.
   */
  public int getId() {
    return id;
  }

  /**
   * Get the start offset for the annotation.
   *
   * @return The start offset for the annotation.
   */
  public int getStartOffset() {
    return startOffset;
  }

  /**
   * Get the end offset for the annotation.
   *
   * @return The end offset for the annotation.
   */
  public int getEndOffset() {
    return endOffset;
  }

  /**
   * Get the metadata for the annotation.
   *
   * @return The metadata for the annotation.
   */
  public String getMetadata() {
    return metadata;
  }

  public String toString() {
    return "[" + annotClass + ", " + annotType + ", " + docId + ", " + id + ", " + startOffset
      + ", " + endOffset + ", " + metadata + "]";
  }

  public boolean equals(Object o) {
    if (o == this)
      return true;

    if (!(o instanceof Annotation)) {
      return false;
    }

    Annotation a = (Annotation) o;

    return docId.equals(a.docId) && annotType.equals(a.annotType) && annotClass.equals(a.annotClass)
      && startOffset == a.startOffset && endOffset == a.endOffset;
  }

  public int hashCode() {
    return docId.hashCode() + annotType.hashCode() + annotClass.hashCode() + startOffset
      + endOffset;
  }
}
