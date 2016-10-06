package edu.berkeley.cs.succinct.annot

import java.io.{ByteArrayInputStream, DataInputStream}

import org.scalatest.FunSuite

class AnnotatedDocumentSerializerSuite extends FunSuite {

  val DELIM = '^'.toByte
  val doc1 = ("doc1", "Document number one",
    "1^ge^word^0^8^foo\n2^ge^space^8^9\n3^ge^word^9^15^bar\n4^ge^space^15^16\n5^ge^word^16^19^baz")
  val doc2 = ("doc2", "Document number two",
    "1^ge^word^0^8\n2^ge^space^8^9\n3^ge^word^9^15\n4^ge^space^15^16\n5^ge^word^16^19")
  val doc3 = ("doc3", "Document number three",
    "1^ge^word^0^8^a\n2^ge^space^8^9\n3^ge^word^9^15^b&c\n4^ge^space^15^16\n5^ge^word^16^21^d^e")
  val data: Seq[(String, String, String)] = Seq(doc1, doc2, doc3)

  test("serialize") {
    val ser = new AnnotatedDocumentSerializer(true)
    ser.serialize(data.iterator)

    // Check docIds
    val docIds = ser.getDocIds
    assert(docIds === Array[String]("doc1", "doc2", "doc3"))

    // Check document text
    val (docOffsets, docText) = ser.getTextBuffer
    assert(docOffsets === Array[Int](0, 20, 40))
    assert(docText === (doc1._2 + "\n" + doc2._2 + "\n" + doc3._2 + "\n").toCharArray)

    // Check document annotations
    val annotData = ser.getAnnotationData
    val annotMap = annotData._1
    val annotDocIdOffsets = annotData._2
    val annotOffsets = annotData._3
    val annotBuffer = annotData._4
    val annotBais = new ByteArrayInputStream(annotBuffer)
    val annotIn = new DataInputStream(annotBais)

    assert(annotMap("^ge^space^") == (0, 3))
    assert(annotMap("^ge^word^") == (3, 6))
    assert(annotDocIdOffsets === Array[Int](0, 1, 2, 0, 1, 2))
    assert(annotOffsets === Array[Int](0, 32, 64, 96, 151, 197))

    Seq(0, 1, 2).foreach(i => {
      {
        val docIdOffset = annotDocIdOffsets(i)
        assert(docIdOffset == i)

        val numEntries = annotIn.readInt()
        assert(numEntries == 2)

        // Range begins
        assert(annotIn.readInt() == 8)
        assert(annotIn.readInt() == 15)

        // Range ends
        assert(annotIn.readInt() == 9)
        assert(annotIn.readInt() == 16)

        // Annotation Ids
        assert(annotIn.readInt() == 2)
        assert(annotIn.readInt() == 4)

        // Metadata
        assert(annotIn.readShort() == 0)
        assert(annotIn.readShort() == 0)
      }
    })

    Seq(3, 4, 5).foreach(i => {
      {
        val docIdOffset = annotDocIdOffsets(i)
        assert(docIdOffset == i - 3)

        val numEntries = annotIn.readInt()
        assert(numEntries == 3)

        // Range begins
        assert(annotIn.readInt() == 0)
        assert(annotIn.readInt() == 9)
        assert(annotIn.readInt() == 16)

        // Range ends
        assert(annotIn.readInt() == 8)
        assert(annotIn.readInt() == 15)
        if (docIds(docIdOffset) == "doc3")
          assert(annotIn.readInt() == 21)
        else
          assert(annotIn.readInt() == 19)

        // Annotation Ids
        assert(annotIn.readInt() == 1)
        assert(annotIn.readInt() == 3)
        assert(annotIn.readInt() == 5)

        // Metadata
        if (docIds(docIdOffset) == "doc1") {
          val len1 = annotIn.readShort()
          val buf1 = new Array[Byte](len1)
          annotIn.read(buf1)
          assert(buf1 === "foo".getBytes())

          val len2 = annotIn.readShort()
          val buf2 = new Array[Byte](len2)
          annotIn.read(buf2)
          assert(buf2 === "bar".getBytes())

          val len3 = annotIn.readShort()
          val buf3 = new Array[Byte](len3)
          annotIn.read(buf3)
          assert(buf3 === "baz".getBytes())
        } else if (docIds(docIdOffset) == "doc2") {
          assert(annotIn.readShort() == 0)
          assert(annotIn.readShort() == 0)
          assert(annotIn.readShort() == 0)
        } else if (docIds(docIdOffset) == "doc3") {
          val len1 = annotIn.readShort()
          val buf1 = new Array[Byte](len1)
          annotIn.read(buf1)
          assert(buf1 === "a".getBytes())

          val len2 = annotIn.readShort()
          val buf2 = new Array[Byte](len2)
          annotIn.read(buf2)
          assert(buf2 === "b&c".getBytes())

          val len3 = annotIn.readShort()
          val buf3 = new Array[Byte](len3)
          annotIn.read(buf3)
          assert(buf3 === "d^e".getBytes())
        }
      }
    })
  }
}
