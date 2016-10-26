package edu.berkeley.cs.succinct.annot.serde

import java.io.{ByteArrayOutputStream, DataOutputStream}

import edu.berkeley.cs.succinct.util.container.IntArrayList

class TempAnnotationBuffers(key: String) extends TempAnnotationData {
  val bufBAOS = new ByteArrayOutputStream()
  val outBuf = new DataOutputStream(bufBAOS)

  val docIdIndexes = new IntArrayList
  val offsets = new IntArrayList

  override def bufStream: DataOutputStream = outBuf

  override def writeDocIdIdx(docIdIdx: Int): Unit = docIdIndexes.add(docIdIdx)

  override def writeOffset(offset: Int): Unit = offsets.add(offset)

  override def close(): Unit = outBuf.close()

  override def read: (Array[Int], Array[Int], Array[Byte]) = {
    (docIdIndexes.toArray, offsets.toArray, bufBAOS.toByteArray)
  }


}
