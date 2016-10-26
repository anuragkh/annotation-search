package edu.berkeley.cs.succinct.annot.serde

import java.io._
import java.net.URLDecoder
import java.util.InvalidPropertiesFormatException

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

class AnnotatedDocumentSerializer(ignoreParseErrors: Boolean = true, conf: (Boolean, File))
  extends Serializable {

  val inMemory = conf._1
  val tempDir = conf._2

  val docIds: ArrayBuffer[String] = new ArrayBuffer[String]
  var curDocTextOffset: Int = 0
  val docTextOffsets: ArrayBuffer[Int] = new ArrayBuffer[Int]
  val docTextOS: StringBuilder = new StringBuilder
  var docAnnotationMap: Map[String, TempAnnotationData] = {
    new TreeMap[String, TempAnnotationData]()
  }

  def getDocIds: Array[String] = docIds.toArray

  def getTextBuffer: (Array[Int], Array[Char]) = (docTextOffsets.toArray, docTextOS.toArray)

  def getAnnotationMap: Map[String, TempAnnotationData] = {
    docAnnotationMap.foreach(_._2.close())
    docAnnotationMap
  }

  def serialize(it: Iterator[(String, String, String)]): Unit = {
    it.foreach(v => addAnnotatedDocument(v._1, v._2, v._3))
  }

  def annotationRecordSize(dat: Array[(Int, Int, Int, String)]): Int = {
    4 + 14 * dat.length + dat.map(i => Math.min(i._4.length, Short.MaxValue)).sum
  }

  def serializeAnnotationRecord(dat: Array[(Int, Int, Int, String)],
                                out: DataOutputStream): Unit = {
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
  }

  def addAnnotations(docIdOffset: Int, docAnnotation: String): Unit = {
    docAnnotation
      .split('\n')
      .map(AnnotatedDocumentSerializer.decodeAnnotationString)
      .groupBy(_._1)
      .foreach(kv => {
        if (!docAnnotationMap.contains(kv._1)) {
          if (inMemory)
            docAnnotationMap += (kv._1 -> new TempAnnotationBuffers(kv._1))
          else
            docAnnotationMap += (kv._1 -> new TempAnnotationFiles(kv._1, tempDir))
        }
        val annotEntry = docAnnotationMap(kv._1)
        val dat = kv._2.map(_._2).sortBy(_._2)
        annotEntry.writeDocIdIdx(docIdOffset)
        annotEntry.writeOffset(annotEntry.bufStream.size())
        serializeAnnotationRecord(dat, annotEntry.bufStream)
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

  def parseAnnotations(docAnnotation: String): Map[String, Array[(Int, Int, Int, String)]] = {
    docAnnotation.split("\n")
      .map(decodeAnnotationString)
      .groupBy(_._1)
      .mapValues(v => v.map(_._2).sortBy(_._2))
  }
}
