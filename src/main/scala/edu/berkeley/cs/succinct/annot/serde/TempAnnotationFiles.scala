package edu.berkeley.cs.succinct.annot.serde

import java.io._

import edu.berkeley.cs.succinct.util.SuccinctConstants

class TempAnnotationFiles(key: String, tmpDir: File) extends TempAnnotationData {
  val bufFile: File = File.createTempFile(key.replace('^', '-'), "tmp-buf", tmpDir)
  bufFile.deleteOnExit()
  val outBuf = new DataOutputStream(new FileOutputStream(bufFile))

  val offsetsFile: File = File.createTempFile(key.replace('^', '-'), "tmp-off")
  offsetsFile.deleteOnExit()
  val outOffsets = new DataOutputStream(new FileOutputStream(offsetsFile))

  val docIdIdxFile: File = File.createTempFile(key.replace('^', '-'), "tmp-dir")
  docIdIdxFile.deleteOnExit()
  val outDocIdIdx = new DataOutputStream(new FileOutputStream(docIdIdxFile))

  def bufStream: DataOutputStream = outBuf

  override def writeOffset(offset: Int): Unit = outOffsets.writeInt(offset)

  override def writeDocIdIdx(docIdIdx: Int): Unit = outDocIdIdx.writeInt(docIdIdx)

  private def readIntArrayFromFile(file: File): Array[Int] = {
    val arr = new Array[Int](file.length().toInt / SuccinctConstants.INT_SIZE_BYTES)
    val in = new DataInputStream(new FileInputStream(file))
    for (i <- arr.indices) {
      arr(i) = in.readInt()
    }
    arr
  }

  private def readByteArrayFromFile(file: File): Array[Byte] = {
    val arr = new Array[Byte](file.length().toInt)
    new DataInputStream(new FileInputStream(file)).readFully(arr)
    arr
  }

  def read: (Array[Int], Array[Int], Array[Byte]) = {
    val docIdIndexes = readIntArrayFromFile(docIdIdxFile)
    val offsets = readIntArrayFromFile(offsetsFile)
    val buf = readByteArrayFromFile(bufFile)

    delete

    (docIdIndexes, offsets, buf)
  }

  def delete: Boolean = bufFile.delete() && offsetsFile.delete() && docIdIdxFile.delete()

  def close(): Unit = {
    outBuf.close()
    outOffsets.close()
    outDocIdIdx.close()
  }

}
