package edu.berkeley.cs.succinct.annot.serde

import java.io._

import edu.berkeley.cs.succinct.util.SuccinctConstants

trait TempAnnotationData {

  var numAnnotations: Long = 0

  def incrementNumAnnotations(num: Long): Unit = numAnnotations += num

  def getNumAnnotations(): Long = numAnnotations

  def bufStream: DataOutputStream

  def writeOffset(offset: Int): Unit

  def writeDocIdIdx(docIdIdx: Int): Unit

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

  def read: (Array[Int], Array[Int], Array[Byte])

  def close(): Unit
}
