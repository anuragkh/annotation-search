package edu.berkeley.cs.succinct.annot

import java.io._
import java.net.URLDecoder
import java.util.InvalidPropertiesFormatException

import edu.berkeley.cs.succinct.util.container.IntArrayList

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

class AnnotatedDocumentSerializer(ignoreParseErrors: Boolean = true,
                                  tempDir: File = new File(System.getProperty("java.io.tmpdir")))
  extends Serializable {

  val docIds: ArrayBuffer[String] = new ArrayBuffer[String]
  var curDocTextOffset: Int = 0
  val docTextOffsets: ArrayBuffer[Int] = new ArrayBuffer[Int]
  val docTextOS: StringBuilder = new StringBuilder
  var docAnnotationMap: Map[String, (IntArrayList, IntArrayList, ByteArrayOutputStream)] = {
    new TreeMap[String, (IntArrayList, IntArrayList, ByteArrayOutputStream)]()
  }

  def getDocIds: Array[String] = docIds.toArray

  def getTextBuffer: (Array[Int], Array[Char]) = (docTextOffsets.toArray, docTextOS.toArray)

  def getAnnotationBuffers: Map[String, (Array[Int], Array[Int], Array[Byte])] = {
    docAnnotationMap.map(kv => (kv._1, (kv._2._1.toArray, kv._2._2.toArray, kv._2._3.toByteArray)))
  }

  def serialize(it: Iterator[(String, String, String)]): Unit = {
    it.foreach(v => addAnnotatedDocument(v._1, v._2, v._3))
  }

  def makeKey(annotClass: String, annotType: String): String = {
    val delim = AnnotatedDocumentSerializer.DELIM
    delim + annotClass + delim + annotType + delim
  }

  def decodeAnnotationString(annotStr: String): (String, (Int, Int, Int, String)) = {
    val e = annotStr.split("\\^", 6)
    val annotKey = makeKey(e(1), e(2))
    val annotId = e(0).toInt
    val startOffset = e(3).toInt
    val endOffset = e(4).toInt
    val metadata = if (e.length == 6) URLDecoder.decode(e(5), "UTF-8") else ""
    (annotKey, (annotId, startOffset, endOffset, metadata))
  }

  def annotationRecordSize(dat: Array[(Int, Int, Int, String)]): Int = {
    4 + 14 * dat.length + dat.map(i => Math.min(i._4.length, Short.MaxValue)).sum
  }

  def serializeAnnotationRecord(dat: Array[(Int, Int, Int, String)],
                                baos: ByteArrayOutputStream): Unit = {
    val out = new DataOutputStream(baos)

    out.writeInt(dat.length)
    dat.map(_._2).foreach(i => out.writeInt(i))
    dat.map(_._3).foreach(i => out.writeInt(i))
    dat.map(_._1).foreach(i => out.writeInt(i))
    dat.map(_._4).foreach(i => {
      if (i.length > Short.MaxValue && !ignoreParseErrors)
        throw new InvalidPropertiesFormatException("Metadata too large: " + i.length)
      val metadata = i.substring(0, Math.min(Short.MaxValue, i.length))
      out.writeShort(metadata.length)
      out.writeBytes(metadata)
    })

    out.close()
  }

  def newAnnotationEntry(key: String): (IntArrayList, IntArrayList, ByteArrayOutputStream) = {
    (new IntArrayList, new IntArrayList, new ByteArrayOutputStream)
  }

  def addAnnotations(docIdOffset: Int, docAnnotation: String): Unit = {
    docAnnotation.split('\n').map(decodeAnnotationString).groupBy(_._1)
      .foreach(kv => {
        if (!docAnnotationMap.contains(kv._1))
          docAnnotationMap += (kv._1 -> newAnnotationEntry(kv._1))
        val annotEntry = docAnnotationMap(kv._1)
        val dat = kv._2.map(_._2).sortBy(_._2)
        annotEntry._1.add(docIdOffset)
        annotEntry._2.add(annotEntry._3.size())
        serializeAnnotationRecord(dat, annotEntry._3)
      })
  }

  def addAnnotatedDocument(docId: String, docText: String, docAnnot: String): Unit = {
    val docIdOffset = docIds.length
    docIds += docId
    docTextOffsets += curDocTextOffset
    docTextOS.append(docText)
    docTextOS.append('\n')
    curDocTextOffset += (docText.length + 1)
    addAnnotations(docIdOffset, docAnnot)
  }
}

object AnnotatedDocumentSerializer {
  val DELIM: Char = '^'
}
