package edu.berkeley.cs.succinct.annot

import java.io._
import java.net.URLDecoder
import java.util.InvalidPropertiesFormatException

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

class AnnotatedDocumentSerializer(ignoreParseErrors: Boolean) extends Serializable {

  val docIds: ArrayBuffer[String] = new ArrayBuffer[String]
  var curDocTextOffset: Int = 0
  val docTextOffsets: ArrayBuffer[Int] = new ArrayBuffer[Int]
  val docTextOS: StringBuilder = new StringBuilder
  var docAnnotationMap: Map[String, (File, DataOutputStream)] = {
    new TreeMap[String, (File, DataOutputStream)]()
  }
  var numAnnotationRecords: Int = 0
  var annotationBufferSize: Int = 0

  def getDocIds: Array[String] = docIds.toArray

  def getTextBuffer: (Array[Int], Array[Char]) = (docTextOffsets.toArray, docTextOS.toArray)

  def getAnnotationData: (Map[String, (Int, Int)], Array[Int], Array[Int], Array[Byte]) = {
    var annotationMap: Map[String, (Int, Int)] = new TreeMap[String, (Int, Int)]()
    val docIdOffsets: Array[Int] = new Array[Int](numAnnotationRecords)
    val annotationOffsets: Array[Int] = new Array[Int](numAnnotationRecords)
    val annotationBuffer: Array[Byte] = new Array[Byte](annotationBufferSize)

    var idx: Int = 0
    var off: Int = 0

    docAnnotationMap.foreach(kv => {
      kv._2._2.close()

      val in: DataInputStream = new DataInputStream(new FileInputStream(kv._2._1))
      val startId = idx
      while (in.available() > 0) {
        docIdOffsets(idx) = in.readInt()
        annotationOffsets(idx) = off
        val recSize = in.readInt()
        try {
          in.read(annotationBuffer, off, recSize)
        } catch {
          case e: Exception => {
            val message = "record size = " + recSize + " off = " + off + " annotationBufferSize = " + annotationBufferSize
            throw new RuntimeException(message, e)
          }
        }
        off += recSize
        idx += 1
      }
      val endId = idx
      annotationMap += (kv._1 ->(startId, endId))
      in.close()
      kv._2._1.delete()
    })

    (annotationMap, docIdOffsets, annotationOffsets, annotationBuffer)
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

  def serializeAnnotationRecord(dat: Array[(Int, Int, Int, String)], out: DataOutputStream): Unit = {
    val recordSize: Int = 4 + 14 * dat.length +
      dat.map(i => Math.min(i._4.getBytes().length, Short.MaxValue)).sum
    out.writeInt(recordSize)
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
    out.flush()
    numAnnotationRecords += 1
    annotationBufferSize += recordSize
  }

  def newAnnotData(key: String): (File, DataOutputStream) = {
    val tmpFile: File = File.createTempFile(key, "tmp")
    tmpFile.deleteOnExit()
    val out: DataOutputStream = new DataOutputStream(new FileOutputStream(tmpFile))
    (tmpFile, out)
  }

  def addAnnotations(docIdOffset: Int, docAnnotation: String): Unit = {
    docAnnotation.split('\n').map(decodeAnnotationString).groupBy(_._1)
      .foreach(kv => {
        if (!docAnnotationMap.contains(kv._1))
          docAnnotationMap += (kv._1 -> newAnnotData(kv._1.replace('^', '-')))
        val annotData = docAnnotationMap(kv._1)
        annotData._2.writeInt(docIdOffset)
        serializeAnnotationRecord(kv._2.map(_._2).sortBy(_._2), annotData._2)
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
