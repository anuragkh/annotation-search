package edu.berkeley.cs.succinct.buffers.annot;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.util.SuccinctConstants;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class AnnotationRecord {
  private static final short THRESHOLD = 32;
  private int offset;
  private String docId;
  private int numEntries;
  private SuccinctAnnotationBuffer buf;
  private long[] metadataMarkers;
  private short[] metadataLengths;

  class AnnotationContext extends SuccinctFile.ExtractContext {
    int idx;

    AnnotationContext() {
      this.marker = -1;
      this.idx = -1;
    }

    AnnotationContext(SuccinctFile.ExtractContext ctx) {
      this.marker = ctx.marker;
      this.idx = 0;
    }
  }

  private AnnotationContext startOffsetCtx;
  private AnnotationContext endOffsetCtx;
  private SuccinctFile.ExtractContext metadataCtx;

  AnnotationRecord(int offset, String docId, int numEntries, SuccinctFile.ExtractContext ctx,
    SuccinctAnnotationBuffer buf) {
    this.offset = offset;
    this.docId = docId;
    this.numEntries = numEntries;
    this.startOffsetCtx = new AnnotationContext(ctx);
    this.endOffsetCtx = new AnnotationContext();
    this.buf = buf;
  }

  private void initMetadataCache() {
    if (metadataCtx == null)
      this.metadataCtx = new SuccinctFile.ExtractContext();

    if (metadataLengths == null)
      this.metadataLengths = new short[numEntries];

    if (metadataMarkers == null) {
      this.metadataMarkers = new long[numEntries];
      for (int i = 0; i < numEntries; i++)
        metadataMarkers[i] = -1;
    }
  }

  /**
   * Get the Annotation Class.
   *
   * @return The Annotation Class.
   */
  private String getAnnotClass() {
    return buf.getAnnotClass();
  }

  /**
   * Get the Annotation Type.
   *
   * @return The Annotation Type.
   */
  private String getAnnotType() {
    return buf.getAnnotType();
  }

  /**
   * Get the document ID for the AnnotationRecord.
   *
   * @return The documentID for the AnnotationRecord.
   */
  String getDocId() {
    return docId;
  }

  /**
   * Get the number of Annotations encoded in the AnnotationRecord.
   *
   * @return The number of Annotations encoded in the AnnotationRecord.
   */
  int getNumEntries() {
    return numEntries;
  }

  /**
   * Get the start offset for the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The start offset.
   */
  int getStartOffset(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }

    if (startOffsetCtx.idx == i) {
      int off = buf.extractInt(startOffsetCtx);
      startOffsetCtx.idx = i + 1;
      return off;
    }

    int rbOffset = offset + i * SuccinctConstants.INT_SIZE_BYTES;
    int off = buf.extractInt(rbOffset, startOffsetCtx);
    startOffsetCtx.idx = i + 1;
    return off;
  }

  /**
   * Get the end offset for the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The end offset.
   */
  int getEndOffset(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }

    if (endOffsetCtx.idx == i) {
      int off = buf.extractInt(endOffsetCtx);
      endOffsetCtx.idx = i + 1;
      return off;
    }

    int reOffset = offset + (numEntries + i) * SuccinctConstants.INT_SIZE_BYTES;
    int off = buf.extractInt(reOffset, endOffsetCtx);
    endOffsetCtx.idx = i + 1;
    return off;
  }

  /**
   * Get the annotation ID for the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The annotation ID.
   */
  int getAnnotId(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }

    int aiOffset = offset + (2 * numEntries + i) * SuccinctConstants.INT_SIZE_BYTES;
    return buf.extractInt(aiOffset);
  }

  /**
   * Get the metadata for the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The annotation metadata.
   */
  String getMetadata(int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }

    initMetadataCache();

    if (metadataMarkers[i] != -1) {
      short length = metadataLengths[i];
      metadataCtx.marker = metadataMarkers[i];
      return buf.extract(metadataCtx, length);
    }

    int j = 0;
    int curOffset = offset + (3 * numEntries) * SuccinctConstants.INT_SIZE_BYTES;
    while (metadataMarkers[j] != -1)
      curOffset += (SuccinctConstants.SHORT_SIZE_BYTES + metadataLengths[j++]);
    metadataCtx.marker = metadataMarkers[j];

    while (true) {
      short length;
      if (metadataCtx.marker == -1) {
        length = buf.extractShort(curOffset, metadataCtx);
      } else {
        length = buf.extractShort(metadataCtx);
      }
      curOffset += SuccinctConstants.SHORT_SIZE_BYTES;

      metadataMarkers[j] = metadataCtx.marker;
      metadataLengths[j] = length;

      if (j == i)
        return buf.extract(metadataCtx, length);

      if (length <= THRESHOLD) {
        buf.extract(metadataCtx, length);
      } else {
        metadataCtx.marker = -1;
      }

      curOffset += length;
      j++;
    }
  }

  /**
   * Get the ith annotation.
   *
   * @param i The index for the annotation.
   * @return The annotation.
   */
  Annotation getAnnotation(final int i) {
    if (i < 0 || i >= numEntries) {
      throw new ArrayIndexOutOfBoundsException("Num entries = " + numEntries + " i = " + i);
    }
    return new Annotation(getAnnotClass(), getAnnotType(), docId, getAnnotId(i), getStartOffset(i),
      getEndOffset(i), getMetadata(i));
  }

  /**
   * Get an iterator over all annotations in the record.
   *
   * @return Iterator over all annotations in the record.
   */
  public Iterator<Annotation> iterator() {
    final Annotation[] annots = new Annotation[numEntries];

    for (int i = 0; i < numEntries; i++) {
      annots[i] = new Annotation(getAnnotClass(), getAnnotType(), getDocId());
      annots[i].setStartOffset(buf.extractInt(startOffsetCtx));
    }

    for (int i = 0; i < numEntries; i++) {
      annots[i].setEndOffset(buf.extractInt(startOffsetCtx));
    }

    for (int i = 0; i < numEntries; i++) {
      annots[i].setId(buf.extractInt(startOffsetCtx));
    }

    for (int i = 0; i < numEntries; i++) {
      short length = buf.extractShort(startOffsetCtx);
      annots[i].setMetadata(buf.extract(startOffsetCtx, length));
    }

    return new Iterator<Annotation>() {
      private int curAnnotIdx = 0;

      @Override public boolean hasNext() {
        return curAnnotIdx < getNumEntries();
      }

      @Override public Annotation next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return annots[curAnnotIdx++];
      }

      @Override public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Find the first start offset <= the given offset.
   *
   * @param offset The offset to search.
   * @return The location of the first start offset <= offset.
   */
  private int firstLEQ(final int offset) {
    int lo = 0, hi = numEntries, arrVal;

    while (lo != hi) {
      int mid = lo + (hi - lo) / 2;
      arrVal = buf.extractInt(this.offset + mid * SuccinctConstants.INT_SIZE_BYTES);
      if (arrVal <= offset)
        lo = mid + 1;
      else
        hi = mid;
    }

    return lo - 1;
  }

  /**
   * Find the first start offset >= the given offset.
   *
   * @param offset The offset to search.
   * @return The location of the first start offset >= offset.
   */
  private int firstGEQ(final int offset) {
    int lo = 0, hi = numEntries, arrVal;

    while (lo != hi) {
      int mid = lo + (hi - lo) / 2;
      arrVal = buf.extractInt(this.offset + mid * SuccinctConstants.INT_SIZE_BYTES);
      if (arrVal < offset)
        lo = mid + 1;
      else
        hi = mid;
    }

    return lo;
  }

  /**
   * Find annotations containing the range (begin, end).
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @return The matching annotations.
   */
  public Annotation[] annotationsContaining(final int begin, final int end) {
    int idx = 0;
    ArrayList<Annotation> res = new ArrayList<>();
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > begin)
        break;
      if (begin >= startOffset && end <= endOffset) {
        res.add(new Annotation(getAnnotClass(), getAnnotType(), docId, 0, startOffset,
          endOffset, getMetadata(idx)));
      }
      idx++;
    }
    return res.toArray(new Annotation[res.size()]);
  }

  /**
   * Checks if any annotation in the record contains the input range.
   *
   * @param begin  Beginning of the input range.
   * @param end    End of the input range.
   * @param filter Filter on annotation metadata.
   * @return True if the record has any annotation containing the range; false otherwise.
   */
  public boolean contains(final int begin, final int end, final AnnotationFilter filter) {
    int idx = 0;
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > begin)
        break;
      if (begin >= startOffset && end <= endOffset && filter.metadataFilter(getMetadata(idx))
        && filter.textFilter(docId, startOffset, endOffset)) {
        return true;
      }
      idx++;
    }
    return false;
  }

  /**
   * Find annotations contained in the range (begin, end).
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @return The matching annotations.
   */
  public Annotation[] annotationsContainedIn(final int begin, final int end) {
    int idx = firstGEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return new Annotation[0];
    }

    ArrayList<Annotation> res = new ArrayList<>();
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > end)
        break;
      if (startOffset >= begin && endOffset <= end) {
        res.add(new Annotation(getAnnotClass(), getAnnotType(), docId, 0, startOffset,
          endOffset, getMetadata(idx)));
      }
      idx++;
    }

    return res.toArray(new Annotation[res.size()]);
  }

  /**
   * Checks if any annotation in the record is contained in the input range.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @return True if the record has any annotation containing the range; false otherwise.
   */
  public boolean containedIn(final int begin, final int end, final AnnotationFilter filter) {
    int idx = firstGEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return false;
    }

    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (startOffset > end)
        break;
      if (startOffset >= begin && endOffset <= end && filter.metadataFilter(getMetadata(idx))
        && filter.textFilter(docId, startOffset, endOffset)) {
        return true;
      }
      idx++;
    }

    return false;
  }

  /**
   * Find annotations before the range (begin, end), within `range` chars of begin.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @param range Max number of chars the annotation can be away from begin; -1 sets the limit to
   *              infinity, i.e., all annotations before.
   * @return The matching annotations.
   */
  public Annotation[] annotationsBefore(final int begin, final int end, final int range) {
    int idx = firstLEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return new Annotation[0];
    }

    ArrayList<Annotation> res = new ArrayList<>();
    while (idx >= 0) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (endOffset <= begin && !(range != -1 && begin - endOffset > range)) {
        res.add(new Annotation(getAnnotClass(), getAnnotType(), docId, 0, startOffset,
          endOffset, getMetadata(idx)));
      }
      idx--;
    }

    return res.toArray(new Annotation[res.size()]);
  }

  /**
   * Checks if any annotation in the record is before the input range, but within `range` characters
   * of the start.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @param range Max number of chars the annotation can be away from end; -1 sets the limit to
   *              infinity, i.e., all annotations after.
   * @return True if the record has any annotation before the range; false otherwise.
   */
  public boolean before(final int begin, final int end, final int range,
    final AnnotationFilter filter) {
    int idx = firstLEQ(begin);
    if (idx < 0 || idx >= numEntries) {
      return false;
    }

    while (idx >= 0) {
      int endOffset = getEndOffset(idx);
      if (endOffset <= begin && !(range != -1 && begin - endOffset > range) && filter
        .metadataFilter(getMetadata(idx)) && filter
        .textFilter(docId, getStartOffset(idx), endOffset)) {
        return true;
      }
      idx--;
    }

    return false;
  }

  /**
   * Find annotations after the range (begin, end), within `range` chars of end.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @param range Max number of chars the annotation can be away from end; -1 sets the limit to
   *              infinity, i.e., all annotations after.
   * @return The matching annotations.
   */
  public Annotation[] annotationsAfter(final int begin, final int end, final int range) {
    int idx = firstGEQ(end);
    if (idx >= numEntries) {
      return new Annotation[0];
    }

    ArrayList<Annotation> res = new ArrayList<>();
    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      int endOffset = getEndOffset(idx);
      if (range != -1 && startOffset - end > range)
        break;
      res.add(new Annotation(getAnnotClass(), getAnnotType(), docId, 0, startOffset,
        endOffset, getMetadata(idx)));
      idx++;
    }

    return res.toArray(new Annotation[res.size()]);
  }

  /**
   * Checks if any annotation in the record is after the input range, but within `range` characters
   * of the end.
   *
   * @param begin Beginning of the input range.
   * @param end   End of the input range.
   * @param range Max number of chars the annotation can be away from end; -1 sets the limit to
   *              infinity, i.e., all annotations after.
   * @return True if the record has any annotation after the range; false otherwise.
   */
  public boolean after(final int begin, final int end, final int range,
    final AnnotationFilter filter) {
    int idx = firstGEQ(end);
    if (idx >= numEntries)
      return false;

    while (idx < numEntries) {
      int startOffset = getStartOffset(idx);
      if (range != -1 && startOffset - end > range)
        break;
      if (filter.metadataFilter(getMetadata(idx)) && filter
        .textFilter(docId, startOffset, getEndOffset(idx)))
        return true;
      idx++;
    }
    return false;
  }

}
